import 'dart:async';
import 'dart:convert';

import 'package:sqlite3/sqlite3.dart';

import 'change.dart';
import 'hlc.dart';
import 'local_store.dart';

/// A [LocalStore] in one SQLite database: records as JSON, the outbox, pull
/// cursors and the client's own state, in four `osync_` tables.
///
/// `sqlite3` 3.x bundles SQLite through build hooks, so Flutter apps need no
/// extra native package; open the file under the app's documents directory.
final class SqliteStore implements LocalStore {
  SqliteStore(this._db) {
    _db.execute('''
      PRAGMA journal_mode = WAL;
      CREATE TABLE IF NOT EXISTS osync_records (
        collection TEXT NOT NULL,
        id TEXT NOT NULL,
        hlc TEXT NOT NULL,
        data TEXT,
        PRIMARY KEY (collection, id)
      );
      CREATE TABLE IF NOT EXISTS osync_outbox (
        seq INTEGER PRIMARY KEY AUTOINCREMENT,
        collection TEXT NOT NULL,
        id TEXT NOT NULL,
        op TEXT NOT NULL,
        hlc TEXT NOT NULL,
        data TEXT,
        UNIQUE (collection, id)
      );
      CREATE TABLE IF NOT EXISTS osync_cursors (
        collection TEXT PRIMARY KEY,
        cursor INTEGER NOT NULL
      );
      CREATE TABLE IF NOT EXISTS osync_meta (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
      );
    ''');
  }

  factory SqliteStore.open(String path) => SqliteStore(sqlite3.open(path));

  factory SqliteStore.inMemory() => SqliteStore(sqlite3.openInMemory());

  final Database _db;
  static final _inTransaction = Object();
  Future<void> _queue = Future.value();

  void close() => _db.close();

  @override
  Future<T> transaction<T>(Future<T> Function() action) {
    if (Zone.current[_inTransaction] == true) return action();

    final done = Completer<T>();
    final previous = _queue;
    _queue = done.future.then((_) {}, onError: (_) {});
    previous.then((_) async {
      _db.execute('BEGIN IMMEDIATE');
      try {
        final result = await runZoned(action, zoneValues: {_inTransaction: true});
        _db.execute('COMMIT');
        done.complete(result);
      } catch (e, s) {
        _db.execute('ROLLBACK');
        done.completeError(e, s);
      }
    });
    return done.future;
  }

  @override
  Future<SyncRecord?> get(String collection, String id) async {
    final rows = _db.select(
      'SELECT collection, id, hlc, data FROM osync_records WHERE collection = ? AND id = ?',
      [collection, id],
    );
    return rows.isEmpty ? null : _record(rows.first);
  }

  @override
  Future<List<SyncRecord>> all(String collection) async => _db
      .select(
        'SELECT collection, id, hlc, data FROM osync_records '
        'WHERE collection = ? AND data IS NOT NULL ORDER BY id',
        [collection],
      )
      .map(_record)
      .toList();

  @override
  Future<void> put(SyncRecord record) async => _db.execute(
        'INSERT INTO osync_records (collection, id, hlc, data) VALUES (?, ?, ?, ?) '
        'ON CONFLICT (collection, id) DO UPDATE SET hlc = excluded.hlc, data = excluded.data',
        [record.collection, record.id, record.hlc.toString(), _encode(record.data)],
      );

  @override
  Future<void> clearSynced(String collection) async => _db.execute(
        'DELETE FROM osync_records WHERE collection = ? '
        'AND id NOT IN (SELECT id FROM osync_outbox WHERE collection = ?)',
        [collection, collection],
      );

  @override
  Future<void> enqueue(String collection, Change change) async => _db.execute(
        // A new seq on replace, so a sync that sent the older change does not
        // clear this one when it finishes.
        'INSERT OR REPLACE INTO osync_outbox (collection, id, op, hlc, data) VALUES (?, ?, ?, ?, ?)',
        [collection, change.id, change.op.name, change.hlc.toString(), _encode(change.data)],
      );

  @override
  Future<List<PendingChange>> pending(String collection, {required int limit}) async => _db
      .select(
        'SELECT seq, collection, id, op, hlc, data FROM osync_outbox '
        'WHERE collection = ? ORDER BY seq LIMIT ?',
        [collection, limit],
      )
      .map(_pending)
      .toList();

  @override
  Future<PendingChange?> pendingFor(String collection, String id) async {
    final rows = _db.select(
      'SELECT seq, collection, id, op, hlc, data FROM osync_outbox WHERE collection = ? AND id = ?',
      [collection, id],
    );
    return rows.isEmpty ? null : _pending(rows.first);
  }

  @override
  Future<int> pendingCount() async => _db.select('SELECT COUNT(*) AS n FROM osync_outbox').first['n'] as int;

  @override
  Future<void> removePending(Iterable<int> seqs) async {
    final stmt = _db.prepare('DELETE FROM osync_outbox WHERE seq = ?');
    try {
      for (final seq in seqs) {
        stmt.execute([seq]);
      }
    } finally {
      stmt.close();
    }
  }

  @override
  Future<int> cursor(String collection) async {
    final rows = _db.select('SELECT cursor FROM osync_cursors WHERE collection = ?', [collection]);
    return rows.isEmpty ? 0 : rows.first['cursor'] as int;
  }

  @override
  Future<void> setCursor(String collection, int cursor) async => _db.execute(
        'INSERT INTO osync_cursors (collection, cursor) VALUES (?, ?) '
        'ON CONFLICT (collection) DO UPDATE SET cursor = excluded.cursor',
        [collection, cursor],
      );

  @override
  Future<String?> meta(String key) async {
    final rows = _db.select('SELECT value FROM osync_meta WHERE key = ?', [key]);
    return rows.isEmpty ? null : rows.first['value'] as String;
  }

  @override
  Future<void> setMeta(String key, String value) async => _db.execute(
        'INSERT INTO osync_meta (key, value) VALUES (?, ?) '
        'ON CONFLICT (key) DO UPDATE SET value = excluded.value',
        [key, value],
      );

  static String? _encode(Map<String, Object?>? data) => data == null ? null : jsonEncode(data);

  static Map<String, Object?>? _decode(Object? text) =>
      text == null ? null : (jsonDecode(text as String) as Map<String, Object?>);

  static SyncRecord _record(Row row) => SyncRecord(
        collection: row['collection'] as String,
        id: row['id'] as String,
        hlc: Hlc.parse(row['hlc'] as String),
        data: _decode(row['data']),
      );

  static PendingChange _pending(Row row) => PendingChange(
        row['seq'] as int,
        row['collection'] as String,
        Change(
          id: row['id'] as String,
          op: SyncOp.parse(row['op'] as String),
          hlc: Hlc.parse(row['hlc'] as String),
          data: _decode(row['data']),
        ),
      );
}
