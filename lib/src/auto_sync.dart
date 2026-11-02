import 'dart:async';
import 'dart:math';

import 'sync_client.dart';
import 'transport.dart';

/// Where background syncing stands, for a status line or a badge.
final class SyncStatus {
  const SyncStatus({
    this.syncing = false,
    this.lastSuccess,
    this.lastError,
    this.nextRetry,
  });

  final bool syncing;
  final DateTime? lastSuccess;
  final Object? lastError;

  /// When a failed sync will be tried again, if it failed.
  final DateTime? nextRetry;
}

/// Keeps a [SyncClient] in sync without the app calling [SyncClient.sync]:
/// shortly after local writes (batched by [debounce]), every [interval], and
/// on [syncNow] — e.g. when the app resumes, the network returns, or a push
/// notification says the server has news. Failures back off exponentially up
/// to [maxBackoff]; an unauthorized answer stops it until [start] is called
/// again with a fresh session.
final class AutoSync {
  AutoSync(
    this.client, {
    this.debounce = const Duration(seconds: 2),
    this.interval = const Duration(minutes: 5),
    this.minBackoff = const Duration(seconds: 2),
    this.maxBackoff = const Duration(minutes: 5),
  });

  final SyncClient client;
  final Duration debounce;
  final Duration interval;
  final Duration minBackoff;
  final Duration maxBackoff;

  final _status = StreamController<SyncStatus>.broadcast();
  SyncStatus _current = const SyncStatus();
  StreamSubscription<RecordChanged>? _writes;
  Timer? _timer;
  Timer? _periodic;
  int _failures = 0;

  Stream<SyncStatus> get status => _status.stream;

  SyncStatus get current => _current;

  bool get running => _writes != null;

  void start() {
    if (running) return;
    _writes = client.changes.where((c) => c.origin == ChangeOrigin.local).listen((_) => _schedule(debounce));
    _periodic = Timer.periodic(interval, (_) => syncNow());
    syncNow();
  }

  void stop() {
    _writes?.cancel();
    _writes = null;
    _periodic?.cancel();
    _periodic = null;
    _timer?.cancel();
    _timer = null;
  }

  /// Syncs now; never throws — the outcome is on [status].
  Future<void> syncNow() async {
    _timer?.cancel();
    _emit(SyncStatus(syncing: true, lastSuccess: _current.lastSuccess));
    try {
      await client.sync();
      _failures = 0;
      _emit(SyncStatus(lastSuccess: DateTime.now()));
    } on Object catch (error) {
      if (error is SyncTransportException && error.unauthorized) {
        stop();
        _emit(SyncStatus(lastSuccess: _current.lastSuccess, lastError: error));
        return;
      }
      final wait = _backoff(++_failures);
      if (running) _schedule(wait);
      _emit(SyncStatus(
        lastSuccess: _current.lastSuccess,
        lastError: error,
        nextRetry: running ? DateTime.now().add(wait) : null,
      ));
    }
  }

  Future<void> dispose() async {
    stop();
    await _status.close();
  }

  void _schedule(Duration after) {
    _timer?.cancel();
    _timer = Timer(after, syncNow);
  }

  Duration _backoff(int failures) {
    final ms = minBackoff.inMilliseconds * pow(2, min(failures - 1, 20));
    return Duration(milliseconds: min(ms.toInt(), maxBackoff.inMilliseconds));
  }

  void _emit(SyncStatus status) {
    _current = status;
    if (!_status.isClosed) _status.add(status);
  }
}
