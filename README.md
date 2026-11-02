# osync-dart

**osync** for Dart and Flutter: the device half of offline-first, two-way sync with a Laravel server running **[osync](https://github.com/prolaxu/osync)**.

## Install

```yaml
dependencies:
  osync:
    git:
      url: https://github.com/prolaxu/osync-dart
      ref: v0.1.0
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
- **Background syncing.** `AutoSync` syncs shortly after local writes (batched), on a timer and on `syncNow()`. It backs off exponentially on failure and stops on 401.
- **Other databases.** Implement `LocalStore` to sync into Isar, Drift or Hive instead of the bundled `SqliteStore`.

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

final auto = AutoSync(sync)..start();                 // debounce + interval + backoff
// on app resume / connectivity regained / push notification:
auto.syncNow();
```

To sync into another database (Isar, Drift), implement `LocalStore`.


## Protocol (v1)

`POST /api/sync` with any auth middleware you configure.

```jsonc
// request
{
  "protocol": 1,
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

An HLC is `<15-digit ms>:<5-digit counter>:<node>`. It is fixed-width up to the node, so string order is clock order on both sides. `reason` is `stale` (with `current`), `invalid` (with `errors`) or `read_only`.


## Tests

```sh
dart pub get && dart test          # 26 tests; e2e skipped
```

End to end, against the real Laravel package:

```sh
# in a checkout of prolaxu/osync
composer install && touch workbench/e2e.sqlite && vendor/bin/testbench serve --port=8787

# here
RLS_E2E_URL=http://127.0.0.1:8787 dart test -t e2e --run-skipped   # 4 tests
```
