import 'hlc.dart';

enum SyncOp {
  upsert,
  delete;

  static SyncOp parse(String value) =>
      values.firstWhere((op) => op.name == value, orElse: () => throw FormatException('Unknown op', value));
}

/// One write to one record, as pushed and pulled.
final class Change {
  const Change({required this.id, required this.op, required this.hlc, this.data});

  factory Change.fromJson(Map<String, Object?> json) {
    final op = SyncOp.parse(json['op'] as String);
    return Change(
      id: json['id'] as String,
      op: op,
      hlc: Hlc.parse(json['hlc'] as String),
      data: op == SyncOp.upsert ? _data(json['data']) : null,
    );
  }

  // PHP encodes an empty map as `[]`.
  static Map<String, Object?> _data(Object? value) => switch (value) {
        Map<String, Object?>() => value,
        null || List<Object?>(isEmpty: true) => <String, Object?>{},
        _ => throw FormatException('Change data must be an object', value),
      };

  final String id;
  final SyncOp op;
  final Hlc hlc;

  /// Null for a delete.
  final Map<String, Object?>? data;

  Map<String, Object?> toJson() => {
        'id': id,
        'op': op.name,
        'hlc': hlc.toString(),
        if (data != null) 'data': data,
      };
}

/// A local change waiting in the outbox; [seq] is its place in the queue.
final class PendingChange {
  const PendingChange(this.seq, this.collection, this.change);

  final int seq;
  final String collection;
  final Change change;
}

/// A record as held on the device. Deleted records stay as tombstones so a
/// late, older write cannot bring them back.
final class SyncRecord {
  const SyncRecord({
    required this.collection,
    required this.id,
    required this.hlc,
    required this.data,
  });

  final String collection;
  final String id;
  final Hlc hlc;

  /// Null when deleted.
  final Map<String, Object?>? data;

  bool get deleted => data == null;
}
