import 'dart:io';

import 'package:osync/osync.dart';
import 'package:test/test.dart';

Change _upsert(String id, String title, int millis) =>
    Change(id: id, op: SyncOp.upsert, hlc: Hlc(millis, 0, 'n'), data: {'title': title});

void main() {
  late SqliteStore store;
  setUp(() => store = SqliteStore.inMemory());
  tearDown(() => store.close());

  test('a newer queued change replaces the older one under a new seq', () async {
    await store.enqueue('notes', _upsert('a', 'v1', 1));
    final first = (await store.pending('notes', limit: 10)).single;
    await store.enqueue('notes', _upsert('a', 'v2', 2));
    final second = (await store.pending('notes', limit: 10)).single;

    expect(second.change.data, {'title': 'v2'});
    expect(second.seq, greaterThan(first.seq));

    await store.removePending([first.seq]);
    expect(await store.pendingCount(), 1);
  });

  test('clearing synced records keeps those with unsent changes', () async {
    for (final id in ['synced', 'unsent']) {
      await store.put(SyncRecord(collection: 'notes', id: id, hlc: Hlc(1, 0, 'n'), data: {'id': id}));
    }
    await store.enqueue('notes', _upsert('unsent', 'x', 1));

    await store.clearSynced('notes');
    expect((await store.all('notes')).map((r) => r.id), ['unsent']);
  });

  test('a failed transaction leaves nothing behind', () async {
    await expectLater(
      store.transaction(() async {
        await store.setCursor('notes', 9);
        throw StateError('boom');
      }),
      throwsStateError,
    );
    expect(await store.cursor('notes'), 0);
  });

  test('transactions queue instead of interleaving', () async {
    final order = <String>[];
    await Future.wait([
      store.transaction(() async {
        order.add('a1');
        await Future<void>.delayed(const Duration(milliseconds: 20));
        order.add('a2');
      }),
      store.transaction(() async => order.add('b')),
    ]);
    expect(order, ['a1', 'a2', 'b']);
  });

  test('survives reopening the file', () async {
    final dir = await Directory.systemTemp.createTemp('rls');
    final path = '${dir.path}/sync.db';
    final a = SqliteStore.open(path);
    await a.put(SyncRecord(collection: 'notes', id: 'x', hlc: Hlc(1, 0, 'n'), data: {'t': 1}));
    await a.setMeta('node_id', 'abc');
    a.close();

    final b = SqliteStore.open(path);
    expect((await b.get('notes', 'x'))?.data, {'t': 1});
    expect(await b.meta('node_id'), 'abc');
    b.close();
    await dir.delete(recursive: true);
  });
}
