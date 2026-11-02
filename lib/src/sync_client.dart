import 'dart:async';
import 'dart:math';

import 'change.dart';
import 'hlc.dart';
import 'local_store.dart';
import 'protocol.dart';
import 'transport.dart';

enum ChangeOrigin { local, remote }

/// A record that changed on the device, from a local write or a sync.
final class RecordChanged {
  const RecordChanged(this.collection, this.id, this.data, this.origin);

  final String collection;
  final String id;

  /// Null when deleted.
  final Map<String, Object?>? data;
  final ChangeOrigin origin;
}

/// What one [SyncClient.sync] did.
final class SyncReport {
  SyncReport();

  int pushed = 0;
  int pulled = 0;
  int rounds = 0;

  /// False when the run stopped at [SyncClient.maxRounds] with more to send
  /// or fetch; call [SyncClient.sync] again (AutoSync does) to carry on.
  bool complete = false;
  final Set<String> reset = {};

  /// Local changes the server refused for good (invalid, read-only). The
  /// local record keeps the value; it will not be pushed again.
  final List<(String collection, Rejection rejection)> refused = [];
}

/// Offline-first sync for a set of collections.
///
/// Writes land in the [LocalStore] and its outbox at once, so the app reads
/// its own writes immediately, online or not. [sync] pushes the outbox and
/// pulls everything past each collection's cursor, repeating until both are
/// drained. Conflicts resolve by hybrid logical clock on both sides, so every
/// device and the server settle on the same latest write.
final class SyncClient {
  SyncClient._(this.store, this.transport, this.collections, this._clock, this.pushBatch, this.maxRounds);

  /// Opens a client over [store], restoring its node id and clock.
  static Future<SyncClient> open({
    required LocalStore store,
    required SyncTransport transport,
    required List<String> collections,
    int pushBatch = 200,
    int maxRounds = 50,
    int Function()? wallClock,
  }) async {
    var node = await store.meta(_nodeKey);
    if (node == null) {
      node = _randomNode();
      await store.setMeta(_nodeKey, node);
    }
    final last = await store.meta(_hlcKey);
    final clock = HlcClock(node, last: last == null ? null : Hlc.parse(last), wallClock: wallClock);
    return SyncClient._(store, transport, List.unmodifiable(collections), clock, pushBatch, maxRounds);
  }

  static final _batchKey = Object();
  static const _nodeKey = 'node_id';
  static const _hlcKey = 'hlc';

  final LocalStore store;
  final SyncTransport transport;
  final List<String> collections;
  final int pushBatch;
  final int maxRounds;
  final HlcClock _clock;

  final _changes = StreamController<RecordChanged>.broadcast();
  Future<SyncReport>? _inFlight;

  String get node => _clock.node;

  Stream<RecordChanged> get changes => _changes.stream;

  Stream<RecordChanged> watch(String collection) => changes.where((c) => c.collection == collection);

  bool get syncing => _inFlight != null;

  Future<Map<String, Object?>?> get(String collection, String id) async =>
      (await store.get(_known(collection), id))?.data;

  Future<List<SyncRecord>> all(String collection) => store.all(_known(collection));

  Future<int> pendingCount() => store.pendingCount();

  Future<void> put(String collection, String id, Map<String, Object?> data) =>
      _write(collection, id, Map.unmodifiable(data));

  Future<void> delete(String collection, String id) => _write(collection, id, null);

  /// Runs [writes] — any number of [put] and [delete] calls — as one
  /// transaction: one disk commit instead of one per write, and nothing
  /// applied if [writes] throws. Change events are emitted after the commit.
  Future<T> batch<T>(Future<T> Function() writes) async {
    if (Zone.current[_batchKey] != null) return writes();
    final events = <RecordChanged>[];
    final result = await runZoned(
      () => store.transaction(writes),
      zoneValues: {_batchKey: events},
    );
    events.forEach(_changes.add);
    return result;
  }

  /// Brings records from an older store into this one, each stamped at its
  /// own edit time ([editedAtMillis]) rather than now, so a conflict with
  /// the server still goes to whichever was really edited last. A null data
  /// imports a delete. Records already held here with a newer stamp are
  /// left alone. Everything imported is queued to push.
  Future<void> importRecords(
    String collection,
    Iterable<({String id, Map<String, Object?>? data, int editedAtMillis})> records,
  ) async {
    _known(collection);
    final list = records.toList();
    if (list.isEmpty) return;
    await store.transaction(() async {
      final held = await store.hlcs(collection, list.map((r) => r.id));
      final pending = await store.pendingFor(collection, list.map((r) => r.id));
      final puts = <SyncRecord>[];
      final deletes = <String>[];
      for (final r in list) {
        final hlc = Hlc(max(r.editedAtMillis, 0), 0, node);
        final newer = [held[r.id], pending[r.id]?.change.hlc].nonNulls.where((h) => h.compareTo(hlc) >= 0);
        if (newer.isNotEmpty) continue;
        _clock.receive(hlc);
        final data = r.data;
        if (data == null) {
          deletes.add(r.id);
        } else {
          puts.add(SyncRecord(collection: collection, id: r.id, hlc: hlc, data: Map.unmodifiable(data)));
        }
        await store.enqueue(
          collection,
          Change(id: r.id, op: data == null ? SyncOp.delete : SyncOp.upsert, hlc: hlc, data: data),
        );
      }
      await store.putMany(puts);
      await store.deleteMany(collection, deletes);
      final last = _clock.last;
      if (last != null) await store.setMeta(_hlcKey, last.toString());
    });
  }

  /// Starts over against an account, e.g. after signing in: every cursor goes
  /// back to zero and every record on the device is queued again, so the
  /// account receives everything here and this device receives everything
  /// in the account. Records the account already holds at the same stamp
  /// cost a no-op on the server.
  Future<void> requeueAll() => store.transaction(() async {
        for (final name in collections) {
          await store.setCursor(name, 0);
          final records = await store.all(name);
          final pending = await store.pendingFor(name, records.map((r) => r.id));
          for (final r in records) {
            if (pending.containsKey(r.id)) continue;
            await store.enqueue(name, Change(id: r.id, op: SyncOp.upsert, hlc: r.hlc, data: r.data));
          }
        }
      });

  /// One full sync. Concurrent calls share the run already going.
  ///
  /// Throws [SyncTransportException] when the server cannot be reached; what
  /// was applied before that stays applied and the rest is retried next time.
  Future<SyncReport> sync() => _inFlight ??= _run().whenComplete(() => _inFlight = null);

  Future<void> close() => _changes.close();

  Future<void> _write(String collection, String id, Map<String, Object?>? data) async {
    _known(collection);
    await store.transaction(() async {
      final hlc = _clock.tick();
      if (data == null) {
        await store.deleteMany(collection, [id]);
      } else {
        await store.putMany([SyncRecord(collection: collection, id: id, hlc: hlc, data: data)]);
      }
      await store.enqueue(
        collection,
        Change(id: id, op: data == null ? SyncOp.delete : SyncOp.upsert, hlc: hlc, data: data),
      );
      await store.setMeta(_hlcKey, hlc.toString());
    });
    final event = RecordChanged(collection, id, data, ChangeOrigin.local);
    final batch = Zone.current[_batchKey] as List<RecordChanged>?;
    batch == null ? _changes.add(event) : batch.add(event);
  }

  Future<SyncReport> _run() async {
    final report = SyncReport();
    var more = true;
    while (more && report.rounds < maxRounds) {
      report.rounds++;
      more = await _round(report);
    }
    report.complete = !more;
    return report;
  }

  /// One request and its answer. Returns whether another round is needed.
  Future<bool> _round(SyncReport report) async {
    final pushes = <String, CollectionPush>{};
    final sent = <int>[];
    for (final name in collections) {
      final pending = await store.pending(name, limit: pushBatch);
      sent.addAll(pending.map((p) => p.seq));
      pushes[name] = CollectionPush(
        cursor: await store.cursor(name),
        changes: [for (final p in pending) p.change],
      );
    }

    final response = await transport.send(SyncRequest(pushes, node: node));
    final applied = <RecordChanged>[];
    var more = false;

    await store.transaction(() async {
      for (final MapEntry(key: name, value: result) in response.collections.entries) {
        if (!collections.contains(name)) continue;
        if (result.reset) {
          await store.clearSynced(name);
          report.reset.add(name);
        }
        final incoming = [
          ...result.changes,
          for (final r in result.rejected)
            if (r.current case final current?) current,
        ];
        for (final r in result.rejected) {
          if (r.current == null) report.refused.add((name, r));
        }
        applied.addAll(await _applyRemote(name, incoming));
        await store.setCursor(name, result.cursor);
        more = more || result.hasMore;
      }
      await store.removePending(sent);
      final last = _clock.last;
      if (last != null) await store.setMeta(_hlcKey, last.toString());
    });

    report.pushed += sent.length;
    report.pulled += applied.length;
    applied.forEach(_changes.add);
    return more || await store.pendingCount() > 0;
  }

  /// Takes each change unless the device already holds the same or a newer
  /// write for that record: two lookups and one pass of writes per page, not
  /// a round of queries per change.
  Future<List<RecordChanged>> _applyRemote(String collection, List<Change> changes) async {
    if (changes.isEmpty) return const [];
    final ids = changes.map((c) => c.id);
    final pending = await store.pendingFor(collection, ids);
    final held = await store.hlcs(collection, ids);

    final latest = <String, Change>{};
    for (final change in changes) {
      _clock.receive(change.hlc);
      final newest = latest[change.id];
      if (newest == null || change.hlc.isAfter(newest.hlc)) latest[change.id] = change;
    }

    final puts = <SyncRecord>[];
    final deletes = <String>[];
    final overridden = <int>[];
    final applied = <RecordChanged>[];
    for (final change in latest.values) {
      final queued = pending[change.id];
      if (queued != null && queued.change.hlc.compareTo(change.hlc) >= 0) continue;
      final local = held[change.id];
      if (local != null && local.compareTo(change.hlc) >= 0) continue;
      if (queued == null && local == null && change.op == SyncOp.delete) continue;

      final data = change.data;
      if (data == null) {
        deletes.add(change.id);
      } else {
        puts.add(SyncRecord(collection: collection, id: change.id, hlc: change.hlc, data: data));
      }
      if (queued != null) overridden.add(queued.seq);
      applied.add(RecordChanged(collection, change.id, data, ChangeOrigin.remote));
    }

    await store.putMany(puts);
    await store.deleteMany(collection, deletes);
    await store.removePending(overridden);
    return applied;
  }

  String _known(String collection) {
    if (!collections.contains(collection)) {
      throw ArgumentError.value(collection, 'collection', 'Not one of $collections');
    }
    return collection;
  }

  static String _randomNode() {
    const alphabet = 'abcdefghijklmnopqrstuvwxyz0123456789';
    final random = Random.secure();
    return 'd${List.generate(15, (_) => alphabet[random.nextInt(alphabet.length)]).join()}';
  }
}
