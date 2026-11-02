@Tags(['bench'])
library;

import 'dart:io';

import 'package:osync/osync.dart';
import 'package:test/test.dart';

import 'fake_server.dart';

void main() {
  test('bench', () async {
    final server = FakeServer();
    for (var i = 0; i < 5000; i++) {
      server.serverEdit('notes', 'n$i', {'title': 'Note $i', 'body': 'x' * 40}, Hlc(1000 + i, 0, 'server'));
    }
    final path = '${Directory.systemTemp.createTempSync('osync').path}/bench.db';
    final store = SqliteStore.open(path);
    final client = await SyncClient.open(store: store, transport: server.transport(), collections: ['notes']);

    var sw = Stopwatch()..start();
    await client.sync();
    final pullMs = sw.elapsedMilliseconds;

    sw = Stopwatch()..start();
    for (var i = 0; i < 1000; i++) {
      await client.put('notes', 'l$i', {'title': 'Local $i'});
    }
    final writeMs = sw.elapsedMilliseconds;

    sw = Stopwatch()..start();
    await client.batch(() async {
      for (var i = 0; i < 1000; i++) {
        await client.put('notes', 'b$i', {'title': 'Batched $i'});
      }
    });
    final batchMs = sw.elapsedMilliseconds;

    sw = Stopwatch()..start();
    await client.sync();
    final pushMs = sw.elapsedMilliseconds;

    sw = Stopwatch()..start();
    final all = await client.all('notes');
    final readMs = sw.elapsedMilliseconds;

    print(
        'BENCH pull5000 ${pullMs}ms | 1000 local writes ${writeMs}ms | 1000 in batch() ${batchMs}ms | push2000 ${pushMs}ms | all(${all.length}) ${readMs}ms');
    store.close();
  });
}
