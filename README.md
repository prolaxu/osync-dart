# osync-dart

**osync** for Dart and Flutter: the device half of offline-first, two-way sync with a Laravel server running **[osync](https://github.com/prolaxu/osync)**.

## Install

```yaml
dependencies:
  osync:
    git:
      url: https://github.com/prolaxu/osync-dart
      ref: v0.4.0
```

```dart
import 'package:osync/osync.dart';
```

Pure Dart, so it works in Flutter, CLI tools and servers. `sqlite3` 3.x bundles SQLite through build hooks, so Flutter needs no extra native package.

## What it does

- **Works offline.** `put`/`delete` write the record and queue it in an outbox in one transaction, so the app reads its own writes at once with or without a network.
- **Incremental pull.** One `sync()` pushes the outbox and pulls only what changed past each collection's cursor, page by page, until both are drained.
- **Handles conflicts.** Every write carries a hybrid logical clock stamp. The later write wins on every device and on the server, even when the phone's clock is wrong or jumps backwards. The clock and node id survive restarts.
- **Safe to retry.** A failed round leaves the outbox intact, and an edit made while a sync is running is never lost.
- **Handles deletes.** Deletes are kept as tombstones, so an older write can't bring a record back. On a server `reset`, synced records are dropped and reloaded, and unsent work is kept.
- **Change streams.** `watch(collection)` emits local and remote changes for the UI.
- **Syncs when it should.** `AutoSync` syncs on app open, on resume, the moment the internet returns, and shortly after local writes (batched). It makes no requests while offline, backs off exponentially on failure, and stops on 401.
- **Other databases.** Implement `LocalStore` to sync into Isar, Drift or Hive instead of the bundled `SqliteStore`.

## Efficiency

Measured with `dart test -t bench --run-skipped` on a file database:

| | v0.1 | now |
|---|---|---|
| Apply a 5,000-change pull | 128 ms | **50–78 ms** |
| 1,000 local writes | 32–45 ms | **17 ms** in one `batch()` |
| Own changes sent back after a push | all of them | **none** |

How:
- **Batched apply.** Each page of incoming changes costs two IN-list lookups (held stamps, queued changes) and one pass of reused prepared statements, not three queries per change.
- **One commit for many writes.** `batch(() async { … })` puts many `put`/`delete` calls in one transaction, so there is one disk commit and nothing is applied if the callback throws. Change events fire after the commit.
- **No tombstones on the device.** The server only ever sends a record's latest change, so a delete removes the row; an unsent local delete lives in the outbox. The database never fills with dead rows.
- **Collapsed outbox.** A record edited ten times offline is pushed once.
- **No echo.** Requests carry the device's node, so the server doesn't return the device's own writes.
- **Gzip both ways.** Request bodies over 1 KB are gzipped (`gzipFrom`), and responses are inflated by `dart:io`.
- **SQLite settings.** WAL with `synchronous = NORMAL` (crash-safe, no fsync per commit), `WITHOUT ROWID` tables keyed the way they're read, and an outbox index in send order.
- **Large backlogs.** A long pull stops at `maxRounds`, and `SyncReport.complete` says whether more is waiting; `AutoSync` carries on by itself.

## Sync on open, on resume and when the internet returns

Pure Dart has no app lifecycle or network events, so the app passes them in. In Flutter, with `connectivity_plus`:

```dart
bool hasNetwork(List<ConnectivityResult> r) => r.any((c) => c != ConnectivityResult.none);

final auto = AutoSync(
  sync,
  isOnline: () async => hasNetwork(await Connectivity().checkConnectivity()),
)..start();                                                     // app open: syncs now if online

final lifecycle = AppLifecycleListener(onResume: auto.appResumed);           // back in foreground
final network = Connectivity().onConnectivityChanged
    .listen((r) => auto.networkChanged(hasNetwork(r)));                      // internet back: sync now

// From an FCM data message saying the server has news:
auto.syncNow();
```

| Event | What `AutoSync` does |
|---|---|
| `start()` (app open) | syncs now if online |
| `appResumed()` | syncs, unless a sync finished within `resumeGap` (30 s) |
| `networkChanged(true)` | syncs now and resets the retry backoff |
| `networkChanged(false)` | drops the pending retry; `status` shows `offline` |
| a local `put`/`delete` | syncs after `debounce` (2 s), batching the burst |
| every `interval` (15 min) | a fallback check for changes made elsewhere |

Watch `auto.status` for a "Syncing… / Offline / Synced 2 min ago" line.

## Usage

```dart
final sync = await SyncClient.open(
  store: SqliteStore.open('${docs.path}/sync.db'),
  collections: ['charts', 'mantras'],
  transport: HttpSyncTransport(
    Uri.parse('https://api.astrobaje.com/api/v1/sync'),
    headers: () async => {'Authorization': 'Bearer ${await token()}'},
  ),
);

await sync.put('charts', chart.id, chart.toJson());   // offline-safe
final charts = await sync.all('charts');
sync.watch('charts').listen((change) => refresh());   // local and remote changes

await sync.batch(() async {                            // many writes, one commit
  for (final c in imported) {
    await sync.put('charts', c.id, c.toJson());
  }
});

final auto = AutoSync(sync)..start();                 // debounce + interval + backoff
// on app resume / connectivity regained / push notification:
auto.syncNow();
```

To sync into another database (Isar, Drift), implement `LocalStore`.

**Moving from an older store.** `importRecords(collection, [(id:, data:, editedAtMillis:)])` brings records in stamped at their own edit time, so a conflict with the server still goes to whichever was really edited last. A `null` data imports a delete.

**Signing in.** `requeueAll()` resets the cursors and queues every local record, so a newly signed-in account receives everything on the device and the device receives everything in the account.


## Protocol (v1)

`POST /api/sync` with any auth middleware you configure.

```jsonc
// request
{
  "protocol": 1,
  "node": "dk3j…",            // this device's HLC node, so its own changes are not sent back
  "collections": {
    "notes": {
      "cursor": 41,
      "changes": [
        { "id": "n1", "op": "upsert", "hlc": "001730000000123:00000:dk3j…", "data": { "title": "…" } },
        { "id": "n2", "op": "delete", "hlc": "001730000000124:00000:dk3j…" }
      ]
    }
  }
}

// response
{
  "protocol": 1,
  "server_time": 1730000000500,
  "collections": {
    "notes": {
      "cursor": 44,
      "reset": false,
      "has_more": false,
      "changes": [ { "id": "n1", "op": "upsert", "hlc": "…", "data": { … } } ],
      "rejected": [ { "id": "n9", "hlc": "…", "reason": "stale", "current": { … } } ]
    }
  }
}
```

An HLC is `<15-digit ms>:<5-digit counter>:<node>`. It is fixed-width up to the node, so string order is clock order on both sides. `reason` is `stale` (with `current`), `invalid` (with `errors`) or `read_only`. A delete has no `data` key, and `rejected` is absent when empty. Bodies may be gzipped (`Content-Encoding: gzip`) in either direction.


## Tests

```sh
dart pub get && dart test          # 37 tests; e2e and bench skipped
dart test -t bench --run-skipped   # the numbers above
```

End to end, against the real Laravel package:

```sh
# in a checkout of prolaxu/osync
composer install && touch workbench/e2e.sqlite && vendor/bin/testbench serve --port=8787

# here
RLS_E2E_URL=http://127.0.0.1:8787 dart test -t e2e --run-skipped   # 5 tests
```

## License

MIT
