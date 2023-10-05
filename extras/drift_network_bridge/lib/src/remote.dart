
@experimental
library drift.network_remote;

import 'package:drift/drift.dart';
import 'package:drift/remote.dart';
import 'package:meta/meta.dart';
import 'package:stream_channel/stream_channel.dart';
import 'network_remote/client_impl.dart';
import 'network_remote/communication.dart';
// ignore: implementation_imports
import 'package:drift/src/remote/protocol.dart';
import 'network_remote/server_impl.dart';


// ignore: subtype_of_sealed_class
/// Serves a drift database connection over any two-way communication channel.
///
/// Users are responsible for creating the underlying stream channels before
/// passing them to this server via [serve].
/// A single drift server can safely handle multiple clients.
@sealed
abstract class DriftNetworkServer implements DriftServer {
  /// Creates a drift server proxying incoming requests to the underlying
  /// [connection].
  ///
  /// If [allowRemoteShutdown] is set to `true` (it defaults to `false`),
  /// clients can use [shutdown] to stop this server remotely.
  /// If [closeConnectionAfterShutdown] is set to `true` (the default), shutting
  /// down the server will also close the [connection].
  factory DriftNetworkServer(QueryExecutor connection,
      {bool allowRemoteShutdown = false,
      bool closeConnectionAfterShutdown = true}) {
    return ServerNetworkImplementation(
        connection, allowRemoteShutdown, closeConnectionAfterShutdown);
  }
}

/// Connects to a remote server over a two-way communication channel.
///
/// The other end of the [channel] must be attached to a drift server with
/// [DriftServer.serve] for this setup to work.
///
/// If it is known that only a single client will connect to this database
/// server, [singleClientMode] can be enabled.
/// When enabled, [shutdown] is implicitly called when the database connection
/// is closed. This may make it easier to dispose the remote isolate or server.
/// Also, update notifications for table updates don't have to be sent which
/// reduces load on the connection.
///
/// If [serialize] is true, drift will only send [bool], [int], [double],
/// [Uint8List], [String] or [List]'s thereof over the channel. Otherwise,
/// the message may be any Dart object.
/// The value of [serialize] for [connectToRemoteAndInitialize] must be the same
/// value passed to [DriftServer.serve].
///
/// The optional [debugLog] can be enabled to print incoming and outgoing
/// messages.
Future<DatabaseConnection> connectToNetworkAndInitialize(
  StreamChannel<Object?> channel, {
  bool debugLog = false,
  bool serialize = true,
  bool singleClientMode = false,
}) async {
  final client = DriftNetworkClient(channel, debugLog, serialize, singleClientMode);
  await client.serverInfo;
  return client.connection;
}

/// Sends a shutdown request over a channel.
///
/// On the remote side, the corresponding channel must have been passed to
/// [DriftServer.serve] for this setup to work.
/// Also, the [DriftServer] must have been configured to allow remote-shutdowns.
Future<void> shutdownOverNetwork(StreamChannel<Object?> channel, {bool serialize = true}) {
  final comm = DriftNetworkCommunication(channel, serialize: serialize);
  return comm
      .request<void>(NoArgsRequest.terminateAll)
      // Sending a terminate request will stop the server, so we won't get a
      // response. This is expected and not an error we should throw.
      .onError<ConnectionClosedException>((error, stackTrace) => null)
      .whenComplete(comm.close);
}
