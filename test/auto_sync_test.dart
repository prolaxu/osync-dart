import 'package:osync/osync.dart';
import 'package:test/test.dart';

import 'fake_server.dart';

void main() {
  late FakeServer server;
  late SyncClient client;
  late SqliteStore store;

  setUp(() async {
    server = FakeServer();
    store = SqliteStore.inMemory();
    client = await SyncClient.open(store: store, transport: server.transport(), collections: ['notes']);
  });
  tearDown(() => store.close());

  Future<void> settle([int ms = 120]) => Future<void>.delayed(Duration(milliseconds: ms));

  test('pushes local writes shortly after they happen, in one batch', () async {
    final auto = AutoSync(client, debounce: const Duration(milliseconds: 30));
    auto.start();
    await settle();
    final before = server.requests;

    await client.put('notes', 'a', {'v': 1});
    await client.put('notes', 'b', {'v': 2});
    await settle();

    expect(server.requests, before + 1);
    expect(await client.pendingCount(), 0);
    await auto.dispose();
  });

  test('backs off and retries after a failure', () async {
    final auto = AutoSync(client, minBackoff: const Duration(milliseconds: 20));
    server.failNext = 2;
    final statuses = <SyncStatus>[];
    auto.status.listen(statuses.add);

    await client.put('notes', 'a', {'v': 1});
    auto.start();
    await settle(200);

    expect(statuses.where((s) => s.lastError != null && s.nextRetry != null), hasLength(2));
    expect(auto.current.lastSuccess, isNotNull);
    expect(await client.pendingCount(), 0);
    await auto.dispose();
  });

  test('stops when the session is no longer valid', () async {
    final auto = AutoSync(client, minBackoff: const Duration(milliseconds: 10));
    server
      ..failNext = 5
      ..failWithStatus = 401;
    auto.start();
    await settle();

    expect(auto.running, isFalse);
    expect((auto.current.lastError as SyncTransportException).unauthorized, isTrue);
    expect(server.requests, 0);
    await auto.dispose();
  });

  test('makes no request while offline, and syncs the moment the network is back', () async {
    var online = false;
    final auto = AutoSync(client, isOnline: () async => online);
    await client.put('notes', 'a', {'v': 1});

    auto.start();
    await settle();
    expect(server.requests, 0);
    expect(auto.current.offline, isTrue);

    online = true;
    await auto.networkChanged(true);
    expect(server.requests, 1);
    expect(await client.pendingCount(), 0);
    expect(auto.current.offline, isFalse);
    await auto.dispose();
  });

  test('losing the network cancels the pending retry', () async {
    var online = true;
    final auto = AutoSync(client, isOnline: () async => online, minBackoff: const Duration(milliseconds: 40));
    server.failNext = 1;
    auto.start();
    await settle(10);
    expect(auto.current.nextRetry, isNotNull);

    online = false;
    await auto.networkChanged(false);
    await settle(100);
    expect(server.requests, 0, reason: 'the retry would have succeeded; it was dropped while offline');
    expect(auto.current.offline, isTrue);
    await auto.dispose();
  });

  test('a resume right after a sync does not sync again; a later one does', () async {
    final auto = AutoSync(client, resumeGap: const Duration(milliseconds: 80));
    auto.start();
    await settle(20);
    expect(server.requests, 1);

    await auto.appResumed();
    expect(server.requests, 1);

    await settle(100);
    await auto.appResumed();
    expect(server.requests, 2);
    await auto.dispose();
  });

  test('pulls a long backlog to the end in one go', () async {
    server = FakeServer(pageSize: 1);
    for (var i = 0; i < 6; i++) {
      server.serverEdit('notes', 'n$i', {'i': i}, Hlc(1000 + i, 0, 'server'));
    }
    final s = SqliteStore.inMemory();
    final c =
        await SyncClient.open(store: s, transport: server.transport(), collections: ['notes'], maxRounds: 2);
    final auto = AutoSync(c);
    await auto.syncNow();
    expect((await c.all('notes')).length, 6);
    s.close();
  });
}
