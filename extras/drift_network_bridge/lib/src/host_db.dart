import 'dart:async';

import 'package:collection/collection.dart';
import 'package:drift/backends.dart';
import 'package:postgres/postgres_v3_experimental.dart';

import 'network/client_connection.dart';
import 'network/host_connection.dart';

/// A drift network database implementation that talks to a host database.
class HostDatabase extends DelegatedDatabase {
  final SqlDialect sqlDialect;
  HostDatabase(
      HostConnection hostConnection,
      {
        bool logStatements = false,
        this.sqlDialect = SqlDialect.sqlite,
      }) : super(hostConnection,
    isSequential: true,
    logStatements: logStatements,
  );

  @override
  SqlDialect get dialect => sqlDialect;
}
