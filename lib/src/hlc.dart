/// A hybrid logical clock stamp: wall-clock milliseconds, a counter for events
/// in the same millisecond, and the node that issued it.
///
/// The string form is fixed-width up to the node, so stamps compare as strings
/// exactly as they compare as clocks — the same rule the server uses.
final class Hlc implements Comparable<Hlc> {
  Hlc(this.millis, this.counter, this.node) {
    if (millis < 0 || counter < 0 || counter > maxCounter || !_node.hasMatch(node)) {
      throw ArgumentError('Invalid HLC parts: $millis, $counter, $node');
    }
  }

  factory Hlc.parse(String value) {
    final m = _pattern.firstMatch(value);
    if (m == null) throw FormatException('Invalid HLC', value);
    return Hlc(int.parse(m[1]!), int.parse(m[2]!), m[3]!);
  }

  static final _pattern = RegExp(r'^(\d{15}):(\d{5}):([A-Za-z0-9_-]{1,64})$');
  static final _node = RegExp(r'^[A-Za-z0-9_-]{1,64}$');
  static const maxCounter = 99999;

  final int millis;
  final int counter;
  final String node;

  @override
  int compareTo(Hlc other) => toString().compareTo(other.toString());

  bool isAfter(Hlc other) => compareTo(other) > 0;

  @override
  bool operator ==(Object other) => other is Hlc && other.toString() == toString();

  @override
  int get hashCode => toString().hashCode;

  @override
  String toString() => '${millis.toString().padLeft(15, '0')}:${counter.toString().padLeft(5, '0')}:$node';
}

/// Issues stamps for one node that never go backwards, even when the device
/// clock does, and that stay ahead of every stamp this node has seen.
final class HlcClock {
  HlcClock(this.node, {Hlc? last, int Function()? wallClock})
      : _last = last,
        _wall = wallClock ?? (() => DateTime.now().millisecondsSinceEpoch);

  final String node;
  final int Function() _wall;
  Hlc? _last;

  Hlc? get last => _last;

  /// A stamp for a local event.
  Hlc tick() => _last = _after(_wall(), _last);

  /// Folds in a stamp from elsewhere, so the next [tick] is later than it.
  void receive(Hlc remote) {
    final last = _last;
    if (last == null || remote.isAfter(last)) {
      _last = Hlc(remote.millis, remote.counter, node);
    }
  }

  Hlc _after(int now, Hlc? after) {
    if (after == null || now > after.millis) return Hlc(now, 0, node);
    return after.counter < Hlc.maxCounter
        ? Hlc(after.millis, after.counter + 1, node)
        : Hlc(after.millis + 1, 0, node);
  }
}
