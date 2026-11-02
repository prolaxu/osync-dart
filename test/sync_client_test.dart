import 'dart:async';

import 'package:osync/osync.dart';
import 'package:test/test.dart';

import 'fake_server.dart';

void main() {
  late FakeServer server;
  final stores = <SqliteStore>[];

  Future<SyncClient> device({int Function()? wall, SqliteStore? store, List<String>? collections}) {
    final s = store ?? SqliteStore.inMemory();
    stores.add(s);
    return SyncClient.open(
      store: s,
      transport: server.transport(),
      collections: collections ?? ['notes'],
      wallClock: wall,
    );
  }

  setUp(() => server = FakeServer());
  tearDown(() {
    for (final s in stores) {
      s.close();
    }
    stores.clear();
  });

  test('a write is readable at once and leaves with the next sync', () async {
    final a = await device();
    await a.put('notes', 'n1', {'title': 'Offline note'});

    expect(await a.get('notes', 'n1'), {'title': 'Offline note'});
    expect(await a.pendingCount(), 1);

    final report = await a.sync();
    expect(report.pushed, 1);
    expect(await a.pendingCount(), 0);
    expect(server.journal['notes']!['n1']!.$2.data, {'title': 'Offline note'});
  });

  test('two devices converge both ways', () async {
    final a = await device();
    final b = await device();

    await a.put('notes', 'n1', {'title': 'From A'});
    await a.sync();
    await b.sync();
    expect(await b.get('notes', 'n1'), {'title': 'From A'});

    await b.put('notes', 'n1', {'title': 'Edited on B'});
    await b.delete('notes', 'n1');
    await b.put('notes', 'n2', {'title': 'New on B'});
    await b.sync();
    await a.sync();

    expect(await a.get('notes', 'n1'), isNull);
    expect((await a.all('notes')).map((r) => r.data), [
      {'title': 'New on B'},
    ]);
  });

  test('concurrent edits settle on the later one everywhere', () async {
    var wall = 1000;
    final a = await device(wall: () => wall);
    final b = await device(wall: () => wall);
    await a.put('notes', 'n1', {'title': 'base'});
    await a.sync();
    await b.sync();

    wall = 2000;
    await a.put('notes', 'n1', {'title': 'A at 2000'});
    wall = 3000;
    await b.put('notes', 'n1', {'title': 'B at 3000'});

    await b.sync();
    final report = await a.sync();
    expect(report.pulled, 1);
    await b.sync();

    expect(await a.get('notes', 'n1'), {'title': 'B at 3000'});
    expect(await b.get('notes', 'n1'), {'title': 'B at 3000'});
    expect(await a.pendingCount(), 0);
  });

  test('a delete is not undone by an older write arriving later', () async {
    var wall = 1000;
    final a = await device(wall: () => wall);
    final b = await device(wall: () => wall);
    await a.put('notes', 'n1', {'title': 'x'});
    await a.sync();
    await b.sync();

    wall = 1500;
    await a.put('notes', 'n1', {'title': 'older edit, still offline'});
    wall = 2000;
    await b.delete('notes', 'n1');
    await b.sync();

    await a.sync();
    expect(await a.get('notes', 'n1'), isNull);
    expect(server.journal['notes']!['n1']!.$2.op, SyncOp.delete);
  });

  test('server edits reach the device', () async {
    final a = await device();
    server.serverEdit('notes', 'web', {'title': 'Made on the website'}, Hlc(5000, 0, 'server'));

    final events = <RecordChanged>[];
    final sub = a.watch('notes').listen(events.add);
    await a.sync();
    await Future<void>.delayed(Duration.zero);
    await sub.cancel();

    expect(await a.get('notes', 'web'), {'title': 'Made on the website'});
    expect(events.single.origin, ChangeOrigin.remote);
  });

  test('a device past the tombstone window resets but keeps unsent work', () async {
    final a = await device();
    final b = await device();
    await a.put('notes', 'kept', {'title': 'kept'});
    await a.put('notes', 'gone', {'title': 'gone'});
    await a.sync();
    await b.sync();

    await a.delete('notes', 'gone');
    await a.sync();
    server.prune();

    await b.put('notes', 'draft', {'title': 'unsent draft'});
    final report = await b.sync();

    expect(report.reset, {'notes'});
    expect((await b.all('notes')).map((r) => r.id), ['draft', 'kept']);
    expect(server.journal['notes']!.containsKey('draft'), isTrue);
  });

  test('pages through a large pull in one sync', () async {
    server = FakeServer(pageSize: 2);
    final a = await device();
    for (var i = 1; i <= 5; i++) {
      await a.put('notes', 'n$i', {'i': i});
    }
    await a.sync();

    final b = await device();
    final report = await b.sync();
    expect((await b.all('notes')).length, 5);
    expect(report.rounds, 3);
  });

  test('says when it stopped at the round limit with more to come', () async {
    server = FakeServer(pageSize: 1);
    for (var i = 0; i < 5; i++) {
      server.serverEdit('notes', 'n$i', {'i': i}, Hlc(1000 + i, 0, 'server'));
    }
    final store = SqliteStore.inMemory();
    stores.add(store);
    final a = await SyncClient.open(
        store: store, transport: server.transport(), collections: ['notes'], maxRounds: 2);

    expect((await a.sync()).complete, isFalse);
    expect((await a.all('notes')).length, 2);
    while (!(await a.sync()).complete) {}
    expect((await a.all('notes')).length, 5);
  });

  test('a failed round keeps the outbox for the next try', () async {
    final a = await device();
    await a.put('notes', 'n1', {'title': 'x'});

    server.failNext = 1;
    await expectLater(a.sync(), throwsA(isA<SyncTransportException>()));
    expect(await a.pendingCount(), 1);

    await a.sync();
    expect(await a.pendingCount(), 0);
  });

  test('a write made while a sync is in flight is not lost', () async {
    final a = await device();
    await a.put('notes', 'n1', {'title': 'v1'});

    final running = a.sync();
    await a.put('notes', 'n1', {'title': 'v2 during sync'});
    await running;

    expect(await a.get('notes', 'n1'), {'title': 'v2 during sync'});
    if (await a.pendingCount() > 0) await a.sync();
    expect(server.journal['notes']!['n1']!.$2.data, {'title': 'v2 during sync'});
  });

  test('concurrent sync calls share one run', () async {
    final a = await device();
    await Future.wait([a.sync(), a.sync(), a.sync()]);
    expect(server.requests, 1);
  });

  test('refused writes are reported and not retried', () async {
    server = FakeServer(readOnly: {'mantras'});
    final a = await device(collections: ['mantras']);
    await a.put('mantras', 'm1', {'text': 'local only'});

    final report = await a.sync();
    expect(report.refused.single.$2.reason, RejectionReason.readOnly);
    expect(await a.pendingCount(), 0);
  });

  test('keeps its node id and clock across restarts', () async {
    var wall = 9000;
    final store = SqliteStore.inMemory();
    final first = await device(store: store, wall: () => wall);
    await first.put('notes', 'n1', {'v': 1});
    final stamp = (await store.get('notes', 'n1'))!.hlc;

    wall = 1000;
    final second = await SyncClient.open(
      store: store,
      transport: server.transport(),
      collections: ['notes'],
      wallClock: () => wall,
    );
    await second.put('notes', 'n1', {'v': 2});

    expect(second.node, first.node);
    expect((await store.get('notes', 'n1'))!.hlc.isAfter(stamp), isTrue);
  });

  test('a batch commits once and announces its writes after the commit', () async {
    final a = await device();
    final events = <String>[];
    final sub = a.changes.listen((e) => events.add(e.id));

    await a.batch(() async {
      for (var i = 0; i < 3; i++) {
        await a.put('notes', 'n$i', {'i': i});
      }
      expect(events, isEmpty);
    });
    await Future<void>.delayed(Duration.zero);
    expect(events, ['n0', 'n1', 'n2']);

    await expectLater(
      a.batch(() async {
        await a.put('notes', 'lost', {'x': 1});
        throw StateError('abort');
      }),
      throwsStateError,
    );
    expect(await a.get('notes', 'lost'), isNull);
    expect(await a.pendingCount(), 3);
    await sub.cancel();
  });

  test('a remote delete leaves no row behind', () async {
    final a = await device();
    final b = await device();
    await a.put('notes', 'n1', {'t': 1});
    await a.sync();
    await b.sync();
    await a.delete('notes', 'n1');
    await a.sync();
    await b.sync();

    expect(await b.all('notes'), isEmpty);
    expect(await b.store.hlcs('notes', ['n1']), isEmpty);
  });

  test('tells the server its node, and is not sent its own changes back', () async {
    final a = await device();
    await a.put('notes', 'n1', {'t': 1});
    final events = <RecordChanged>[];
    final sub = a.changes.where((e) => e.origin == ChangeOrigin.remote).listen(events.add);
    final report = await a.sync();
    await Future<void>.delayed(Duration.zero);

    expect(server.received.single.node, a.node);
    expect(report.pulled, 0);
    expect(events, isEmpty);
    await sub.cancel();
  });

  test('imports old records at their own edit times, so the later edit still wins', () async {
    final a = await device(wall: () => 5000);
    final b = await device(wall: () => 5000);
    await b.put('notes', 'edited-later-on-b', {'v': 'b'});
    await b.sync();

    await a.importRecords('notes', [
      (id: 'edited-later-on-b', data: {'v': 'old copy on a'}, editedAtMillis: 1000),
      (id: 'only-on-a', data: {'v': 'a'}, editedAtMillis: 2000),
      (id: 'deleted-on-a', data: null, editedAtMillis: 3000),
    ]);
    expect(await a.pendingCount(), 3);
    await a.sync();

    expect(await a.get('notes', 'edited-later-on-b'), {'v': 'b'});
    expect(await a.get('notes', 'only-on-a'), {'v': 'a'});
    expect(server.journal['notes']!['deleted-on-a']!.$2.op, SyncOp.delete);
    expect((await a.store.get('notes', 'only-on-a'))!.hlc.millis, 2000);
  });

  test('requeueAll hands everything to a newly signed-in account and pulls its records', () async {
    final a = await device();
    await a.put('notes', 'guest', {'v': 1});
    await a.sync();

    server = FakeServer();
    server.serverEdit('notes', 'account', {'v': 'from account'}, Hlc(1, 0, 'server'));
    final b = await SyncClient.open(store: a.store, transport: server.transport(), collections: ['notes']);
    await b.requeueAll();
    await b.sync();

    expect(server.journal['notes']!.containsKey('guest'), isTrue);
    expect(await b.get('notes', 'account'), {'v': 'from account'});
  });

  test('refuses collections it was not opened with', () async {
    final a = await device();
    expect(() => a.put('ghosts', 'x', {}), throwsArgumentError);
  });
}
