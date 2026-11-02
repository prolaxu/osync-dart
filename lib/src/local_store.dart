import 'change.dart';
import 'hlc.dart';

/// The device database the client syncs into.
///
/// [SqliteStore] is the ready-made one; implement this to sync into another
/// database (Isar, Drift, Hive). The client calls the bulk methods once per
/// page of changes, so an implementation should do each in a few statements.
abstract interface class LocalStore {
  /// Runs [action] atomically. Calls made from inside it join the same
  /// transaction.
  Future<T> transaction<T>(Future<T> Function() action);

  Future<SyncRecord?> get(String collection, String id);

  Future<List<SyncRecord>> all(String collection);

  /// The stamps of those of [ids] the device holds.
  Future<Map<String, Hlc>> hlcs(String collection, Iterable<String> ids);

  /// Inserts or replaces each record.
  Future<void> putMany(List<SyncRecord> records);

  Future<void> deleteMany(String collection, Iterable<String> ids);

  /// Drops the collection's records, except those with a change still in the
  /// outbox — those are newer than anything the server can send.
  Future<void> clearSynced(String collection);

  /// Queues [change]; an older queued change for the same record is replaced.
  Future<void> enqueue(String collection, Change change);

  Future<List<PendingChange>> pending(String collection, {required int limit});

  /// The queued changes for those of [ids] that have one.
  Future<Map<String, PendingChange>> pendingFor(String collection, Iterable<String> ids);

  Future<int> pendingCount();

  Future<void> removePending(Iterable<int> seqs);

  Future<int> cursor(String collection);

  Future<void> setCursor(String collection, int cursor);

  Future<String?> meta(String key);

  Future<void> setMeta(String key, String value);
}
