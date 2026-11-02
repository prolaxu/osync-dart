import 'dart:async';
import 'dart:math';

import 'sync_client.dart';
import 'transport.dart';

/// Where background syncing stands, for a status line or a badge.
final class SyncStatus {
  const SyncStatus({
    this.syncing = false,
    this.offline = false,
    this.lastSuccess,
    this.lastError,
    this.nextRetry,
  });

  final bool syncing;

  /// No network: nothing is attempted until [AutoSync.networkChanged] says
  /// it is back.
  final bool offline;
  final DateTime? lastSuccess;
  final Object? lastError;

  /// When a failed sync will be tried again, if it failed.
  final DateTime? nextRetry;
}

/// Keeps a [SyncClient] in sync without the app calling [SyncClient.sync].
///
/// It syncs:
/// - when the app opens ([start]) and returns to the foreground
///   ([appResumed]), skipping a resume within [resumeGap] of the last
///   success — a share sheet or permission prompt is not a new session;
/// - the moment the network comes back ([networkChanged]);
/// - shortly after local writes, batched by [debounce];
/// - every [interval] while running, as a fallback for changes made
///   elsewhere — or on [syncNow], e.g. from a push notification.
///
/// With [isOnline] given, no request is made while offline, so a phone in a
/// tunnel neither burns retries nor drains the battery. Failures back off
/// exponentially up to [maxBackoff]; an unauthorized answer stops it until
/// [start] is called again with a fresh session.
final class AutoSync {
  AutoSync(
    this.client, {
    this.isOnline,
    this.debounce = const Duration(seconds: 2),
    this.interval = const Duration(minutes: 15),
    this.resumeGap = const Duration(seconds: 30),
    this.minBackoff = const Duration(seconds: 2),
    this.maxBackoff = const Duration(minutes: 5),
  });

  final SyncClient client;

  /// Whether the device has a network now; null assumes it always does.
  final Future<bool> Function()? isOnline;
  final Duration debounce;
  final Duration interval;
  final Duration resumeGap;
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

  /// Starts syncing; call when the app opens. Syncs at once if online.
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

  /// The app is back in the foreground.
  Future<void> appResumed() async {
    if (!running) return;
    final last = _current.lastSuccess;
    if (last != null && DateTime.now().difference(last) < resumeGap) return;
    await syncNow();
  }

  /// The network came or went. Coming back syncs at once, without waiting
  /// out a backoff earned while it was down.
  Future<void> networkChanged(bool online) async {
    if (!running) return;
    if (!online) {
      _timer?.cancel();
      _emit(SyncStatus(offline: true, lastSuccess: _current.lastSuccess));
      return;
    }
    _failures = 0;
    await syncNow();
  }

  /// Syncs now if online; never throws — the outcome is on [status].
  Future<void> syncNow() async {
    _timer?.cancel();
    if (isOnline != null && !await isOnline!()) {
      _emit(SyncStatus(offline: true, lastSuccess: _current.lastSuccess));
      return;
    }

    _emit(SyncStatus(syncing: true, lastSuccess: _current.lastSuccess));
    try {
      var report = await client.sync();
      while (!report.complete) {
        report = await client.sync();
      }
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
