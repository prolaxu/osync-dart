@Tags(['e2e'])
library;

import 'dart:convert';
import 'dart:io';

import 'package:http/http.dart' as http;
import 'package:osync/osync.dart';
import 'package:test/test.dart';

/// Against the real Laravel package. Start it from the repo root with
/// `vendor/bin/testbench serve --port=8787`, then run
/// `RLS_E2E_URL=http://127.0.0.1:8787 dart test -t e2e`.
void main() {
  final base = Platform.environment['RLS_E2E_URL'];
  final stores = <SqliteStore>[];

  Future<SyncClient> device(String user, {List<String> collections = const ['notes']}) {
    final store = SqliteStore.inMemory();
    stores.add(store);
    return SyncClient.open(
      store: store,
      collections: collections,
      transport: HttpSyncTransport(
        Uri.parse('$base/api/sync'),
        headers: () async => {'Authorization': 'Bearer $user'},
      ),
    );
  }

  Future<void> server(String path, [Map<String, String> body = const {}]) async {
    final response =
        await http.post(Uri.parse('$base$path'), headers: {'Accept': 'application/json'}, body: body);
    expect(response.statusCode, anyOf(200, 201), reason: response.body);
  }

  setUp(() => server('/e2e/reset'));
  tearDown(() {
    for (final s in stores) {
      s.close();
    }
    stores.clear();
  });

  test('phones and the website converge through the real server', () async {
    final phone = await device('ram');
    final tablet = await device('ram');

    for (var i = 1; i <= 7; i++) {
      await phone.put('notes', 'n$i', {'title': 'Kundali $i', 'body': null});
    }
    await phone.sync();

    final pulled = await tablet.sync();
    expect(pulled.rounds, greaterThan(2), reason: 'page size 3 forces paging');
    expect((await tablet.all('notes')).length, 7);

    await tablet.put('notes', 'n1', {'title': 'Renamed on tablet', 'body': 'notes'});
    await tablet.delete('notes', 'n2');
    await tablet.sync();
    await phone.sync();
    expect(await phone.get('notes', 'n1'), {'title': 'Renamed on tablet', 'body': 'notes'});
    expect(await phone.get('notes', 'n2'), isNull);

    await server('/e2e/web-edit', {'id': 'n3', 'title': 'Edited on the website'});
    await phone.sync();
    expect((await phone.get('notes', 'n3'))?['title'], 'Edited on the website');
  });

  test('the later of two offline edits wins on every device', () async {
    final phone = await device('ram');
    final tablet = await device('ram');
    await phone.put('notes', 'n1', {'title': 'base', 'body': null});
    await phone.sync();
    await tablet.sync();

    await phone.put('notes', 'n1', {'title': 'phone edit', 'body': null});
    await Future<void>.delayed(const Duration(milliseconds: 5));
    await tablet.put('notes', 'n1', {'title': 'tablet edit, later', 'body': null});

    await tablet.sync();
    await phone.sync();

    expect((await phone.get('notes', 'n1'))?['title'], 'tablet edit, later');
    expect((await tablet.get('notes', 'n1'))?['title'], 'tablet edit, later');
    expect(await phone.pendingCount(), 0);
  });

  test('users see only their own records; content is read-only', () async {
    final ram = await device('ram', collections: ['notes', 'mantras']);
    final sita = await device('sita', collections: ['notes', 'mantras']);

    await ram.put('notes', 'private', {'title': 'Ram only', 'body': null});
    await ram.put('mantras', 'fake', {'text': 'not allowed'});
    final report = await ram.sync();
    expect(report.refused.single.$2.reason, RejectionReason.readOnly);

    await server('/e2e/publish-mantra', {'text': 'Om Namah Shivaya'});
    await sita.sync();

    expect(await sita.all('notes'), isEmpty);
    expect((await sita.all('mantras')).map((r) => r.data?['text']), ['Om Namah Shivaya']);
  });

  test('invalid data comes back refused with the field errors', () async {
    final phone = await device('ram');
    await phone.put('notes', 'bad', {'title': ''});

    final report = await phone.sync();
    final (_, rejection) = report.refused.single;
    expect(rejection.reason, RejectionReason.invalid);
    expect(rejection.errors.keys, contains('title'));
    expect(jsonEncode(await phone.get('notes', 'bad')), '{"title":""}');
  });
}
