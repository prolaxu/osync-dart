import 'change.dart';

const syncProtocolVersion = 1;

enum RejectionReason {
  /// A newer write holds the record; [Rejection.current] is the winner.
  stale,
  invalid,
  readOnly;

  static RejectionReason parse(String value) => switch (value) {
        'stale' => stale,
        'invalid' => invalid,
        'read_only' => readOnly,
        _ => throw FormatException('Unknown rejection reason', value),
      };
}

final class Rejection {
  const Rejection({
    required this.id,
    required this.reason,
    this.current,
    this.errors = const {},
  });

  factory Rejection.fromJson(Map<String, Object?> json) => Rejection(
        id: json['id'] as String,
        reason: RejectionReason.parse(json['reason'] as String),
        current: json['current'] == null ? null : Change.fromJson(json['current'] as Map<String, Object?>),
        errors: (json['errors'] as Map<String, Object?>? ?? const {}).map(
          (field, messages) => MapEntry(field, (messages as List<Object?>).cast<String>()),
        ),
      );

  final String id;
  final RejectionReason reason;
  final Change? current;
  final Map<String, List<String>> errors;
}

final class CollectionResult {
  const CollectionResult({
    required this.cursor,
    required this.changes,
    required this.hasMore,
    required this.reset,
    required this.rejected,
  });

  factory CollectionResult.fromJson(Map<String, Object?> json) => CollectionResult(
        cursor: json['cursor'] as int,
        reset: json['reset'] as bool,
        hasMore: json['has_more'] as bool,
        changes: (json['changes'] as List<Object?>)
            .map((c) => Change.fromJson(c as Map<String, Object?>))
            .toList(),
        rejected: (json['rejected'] as List<Object?>)
            .map((r) => Rejection.fromJson(r as Map<String, Object?>))
            .toList(),
      );

  final int cursor;
  final List<Change> changes;
  final bool hasMore;

  /// The server no longer has the deletes since this device's cursor: drop
  /// the collection's synced records and take [changes] as the full set.
  final bool reset;
  final List<Rejection> rejected;
}

final class CollectionPush {
  const CollectionPush({required this.cursor, required this.changes});

  final int cursor;
  final List<Change> changes;

  Map<String, Object?> toJson() => {
        'cursor': cursor,
        'changes': [for (final c in changes) c.toJson()],
      };
}

final class SyncRequest {
  const SyncRequest(this.collections);

  final Map<String, CollectionPush> collections;

  Map<String, Object?> toJson() => {
        'protocol': syncProtocolVersion,
        'collections': {for (final e in collections.entries) e.key: e.value.toJson()},
      };
}

final class SyncResponse {
  const SyncResponse({required this.serverTime, required this.collections});

  factory SyncResponse.fromJson(Map<String, Object?> json) => SyncResponse(
        serverTime: json['server_time'] as int,
        collections: (json['collections'] as Map<String, Object?>).map(
          (name, result) => MapEntry(name, CollectionResult.fromJson(result as Map<String, Object?>)),
        ),
      );

  final int serverTime;
  final Map<String, CollectionResult> collections;
}
