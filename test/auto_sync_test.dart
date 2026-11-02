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
}
