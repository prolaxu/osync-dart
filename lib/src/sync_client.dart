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
      final change = Change(id: id, op: data == null ? SyncOp.delete : SyncOp.upsert, hlc: hlc, data: data);
      await store.put(SyncRecord(collection: collection, id: id, hlc: hlc, data: data));
      await store.enqueue(collection, change);
      await store.setMeta(_hlcKey, hlc.toString());
    });
    _changes.add(RecordChanged(collection, id, data, ChangeOrigin.local));
  }

  Future<SyncReport> _run() async {
    final report = SyncReport();
    var more = true;
    while (more && report.rounds < maxRounds) {
      report.rounds++;
      more = await _round(report);
    }
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

    final response = await transport.send(SyncRequest(pushes));
    final applied = <RecordChanged>[];
    var more = false;

    await store.transaction(() async {
      for (final MapEntry(key: name, value: result) in response.collections.entries) {
        if (!collections.contains(name)) continue;
        if (result.reset) {
          await store.clearSynced(name);
          report.reset.add(name);
        }
        for (final change in result.changes) {
          if (await _applyRemote(name, change)) {
            applied.add(RecordChanged(name, change.id, change.data, ChangeOrigin.remote));
          }
        }
        for (final rejection in result.rejected) {
          final current = rejection.current;
          if (current != null) {
            if (await _applyRemote(name, current)) {
              applied.add(RecordChanged(name, current.id, current.data, ChangeOrigin.remote));
            }
          } else {
            report.refused.add((name, rejection));
          }
        }
        await store.setCursor(name, result.cursor);
        more = more || result.hasMore;
      }
      await store.removePending(sent);
      final last = _clock.last;
      if (last != null) await store.setMeta(_hlcKey, last.toString());
    });

    report.pushed += sent.length;
    report.pulled += applied.length;
    for (final event in applied) {
      _changes.add(event);
    }
    return more || await store.pendingCount() > 0;
  }

  /// Takes [change] unless the device already holds the same or a newer
  /// write. Returns whether the record changed.
  Future<bool> _applyRemote(String collection, Change change) async {
    _clock.receive(change.hlc);

    final pending = await store.pendingFor(collection, change.id);
    if (pending != null && pending.change.hlc.compareTo(change.hlc) >= 0) return false;

    final local = await store.get(collection, change.id);
    if (local != null && local.hlc.compareTo(change.hlc) >= 0) return false;

    await store.put(SyncRecord(collection: collection, id: change.id, hlc: change.hlc, data: change.data));
    if (pending != null) await store.removePending([pending.seq]);
    return true;
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
