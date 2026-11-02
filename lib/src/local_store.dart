import 'change.dart';

/// The device database the client syncs into.
///
/// [SqliteStore] is the ready-made one; implement this to sync into another
/// database (Isar, Drift, Hive). Every method is called inside [transaction]
/// when the client needs several writes to land together.
abstract interface class LocalStore {
  /// Runs [action] atomically. Calls made from inside it join the same
  /// transaction.
  Future<T> transaction<T>(Future<T> Function() action);

  /// The record, including a tombstone, or null if never seen.
  Future<SyncRecord?> get(String collection, String id);

  /// Live records only.
  Future<List<SyncRecord>> all(String collection);

  Future<void> put(SyncRecord record);

  /// Drops the collection's records, except those with a change still in the
  /// outbox — those are newer than anything the server can send.
  Future<void> clearSynced(String collection);

  /// Queues [change]; an older queued change for the same record is replaced.
  Future<void> enqueue(String collection, Change change);

  Future<List<PendingChange>> pending(String collection, {required int limit});

  Future<PendingChange?> pendingFor(String collection, String id);

  Future<int> pendingCount();

  Future<void> removePending(Iterable<int> seqs);

  Future<int> cursor(String collection);

  Future<void> setCursor(String collection, int cursor);

  Future<String?> meta(String key);

  Future<void> setMeta(String key, String value);
}
