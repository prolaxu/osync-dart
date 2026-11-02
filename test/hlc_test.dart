import 'package:osync/osync.dart';
import 'package:test/test.dart';

void main() {
  test('string form matches the server byte for byte', () {
    final hlc = Hlc(1730000000123, 7, 'phone-a');
    expect(hlc.toString(), '001730000000123:00007:phone-a');
    expect(Hlc.parse(hlc.toString()), hlc);
  });

  test('orders as strings the way it orders as a clock', () {
    final stamps = [
      '000000000010000:00000:a',
      '000000000000005:00001:a',
      '000000000000005:00000:b',
      '000000000000005:00000:a'
    ].map(Hlc.parse).toList()
      ..sort();
    expect(stamps.map((s) => s.toString()), [
      '000000000000005:00000:a',
      '000000000000005:00000:b',
      '000000000000005:00001:a',
      '000000000010000:00000:a',
    ]);
  });

  test('never runs backwards when the wall clock does', () {
    var wall = 5000;
    final clock = HlcClock('n', wallClock: () => wall);
    final first = clock.tick();
    wall = 1000;
    final second = clock.tick();
    final third = clock.tick();
    expect(second.isAfter(first), isTrue);
    expect(third.isAfter(second), isTrue);
    expect(second.millis, 5000);
  });

  test('stays ahead of stamps it has received', () {
    final clock = HlcClock('n', wallClock: () => 1000);
    final remote = Hlc(9000, 4, 'other');
    clock.receive(remote);
    expect(clock.tick().isAfter(remote), isTrue);
  });

  test('rejects malformed stamps', () {
    for (final bad in ['', '1:2:a', '000000000000005:00000:', '000000000000005:00000:a b']) {
      expect(() => Hlc.parse(bad), throwsFormatException, reason: bad);
    }
  });
}
