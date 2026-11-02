import 'package:osync/osync.dart';

/// The server's protocol in memory, for tests: one scope, a collapsed journal
/// per collection, last-write-wins by HLC, paging and tombstone compaction.
final class FakeServer {
  FakeServer({this.pageSize = 500, this.readOnly = const {}});

  final int pageSize;
  final Set<String> readOnly;
  int version = 0;
  int compactedThrough = 0;
  final Map<String, Map<String, (int, Change)>> journal = {};
  int requests = 0;

  /// Makes the next request fail as if the network dropped.
  int failNext = 0;
  int? failWithStatus;

  SyncTransport transport() => _FakeTransport(this);

  /// An edit made on the server side, stamped to win.
  void serverEdit(String collection, String id, Map<String, Object?>? data, Hlc hlc) {
    _write(
        collection, Change(id: id, op: data == null ? SyncOp.delete : SyncOp.upsert, hlc: hlc, data: data));
  }

  /// Drops every tombstone, as `osync:prune` does past the retention window.
  void prune() {
    for (final records in journal.values) {
      records.removeWhere((_, entry) {
        final (v, change) = entry;
        if (change.op != SyncOp.delete) return false;
        if (v > compactedThrough) compactedThrough = v;
        return true;
      });
    }
  }

  SyncResponse handle(SyncRequest request) {
    requests++;
    final out = <String, CollectionResult>{};
    for (final MapEntry(key: name, value: push) in request.collections.entries) {
      final records = journal.putIfAbsent(name, () => {});
      final rejected = <Rejection>[];
      for (final change in push.changes) {
        if (readOnly.contains(name)) {
          rejected.add(Rejection(id: change.id, reason: RejectionReason.readOnly));
          continue;
        }
        final current = records[change.id]?.$2;
        if (current != null && current.hlc.compareTo(change.hlc) >= 0) {
          if (current.hlc != change.hlc) {
            rejected.add(Rejection(id: change.id, reason: RejectionReason.stale, current: current));
          }
          continue;
        }
        _write(name, change);
      }
      final reset = push.cursor > 0 && push.cursor < compactedThrough;
      final from = reset ? 0 : push.cursor;
      final rows = records.values.where((e) => e.$1 > from).toList()..sort((a, b) => a.$1.compareTo(b.$1));
      final page = rows.take(pageSize).toList();
      out[name] = CollectionResult(
        cursor: page.isEmpty ? from : page.last.$1,
        changes: [for (final (_, c) in page) c],
        hasMore: rows.length > pageSize,
        reset: reset,
        rejected: rejected,
      );
    }
    return SyncResponse(serverTime: 0, collections: out);
  }

  void _write(String collection, Change change) {
    journal.putIfAbsent(collection, () => {})[change.id] = (++version, change);
  }
}

final class _FakeTransport implements SyncTransport {
  _FakeTransport(this.server);

  final FakeServer server;

  @override
  Future<SyncResponse> send(SyncRequest request) async {
    await Future<void>.delayed(Duration.zero);
    if (server.failNext > 0) {
      server.failNext--;
      throw SyncTransportException('offline', statusCode: server.failWithStatus);
    }
    return server.handle(request);
  }
}
