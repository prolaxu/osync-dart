import 'dart:convert';

import 'package:http/http.dart' as http;

import 'protocol.dart';

/// Carries one sync round to the server. [HttpSyncTransport] is the real one;
/// tests and other channels (a WebSocket, a background isolate) supply theirs.
abstract interface class SyncTransport {
  Future<SyncResponse> send(SyncRequest request);
}

/// The server could not be reached or refused the round. Local data is
/// untouched; the round is safe to retry.
class SyncTransportException implements Exception {
  const SyncTransportException(this.message, {this.statusCode});

  final String message;
  final int? statusCode;

  bool get unauthorized => statusCode == 401;

  /// Worth retrying later: offline, a timeout, or a server-side failure.
  bool get retryable => statusCode == null || statusCode! >= 500 || statusCode == 429;

  @override
  String toString() => 'SyncTransportException(${statusCode ?? 'network'}): $message';
}

final class HttpSyncTransport implements SyncTransport {
  HttpSyncTransport(
    this.endpoint, {
    http.Client? client,
    this.headers,
    this.timeout = const Duration(seconds: 30),
  }) : _client = client ?? http.Client();

  final Uri endpoint;

  /// Extra headers per request, e.g. a fresh `Authorization: Bearer …`.
  final Future<Map<String, String>> Function()? headers;
  final Duration timeout;
  final http.Client _client;

  @override
  Future<SyncResponse> send(SyncRequest request) async {
    final http.Response response;
    try {
      response = await _client
          .post(
            endpoint,
            headers: {
              'Accept': 'application/json',
              'Content-Type': 'application/json',
              ...?await headers?.call(),
            },
            body: jsonEncode(request.toJson()),
          )
          .timeout(timeout);
    } on Exception catch (e) {
      throw SyncTransportException(e.toString());
    }

    if (response.statusCode != 200) {
      throw SyncTransportException(response.body, statusCode: response.statusCode);
    }
    try {
      return SyncResponse.fromJson(jsonDecode(response.body) as Map<String, Object?>);
    } on Object catch (e) {
      throw SyncTransportException('Malformed sync response: $e', statusCode: response.statusCode);
    }
  }

  void close() => _client.close();
}
