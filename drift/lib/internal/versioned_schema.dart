/// Defines base classes to generate lightweight tables and views. This library
/// is used by code generated via `drift_dev schema steps` to generate snapshots
/// of every schema version of your database without much overhead.
///
/// For more information on how to use that feature, see
/// https://drift.simonbinder.eu/docs/advanced-features/migrations/#step-by-step
///
/// __Warning:__ This library is not meant to be imported into user-written
/// code, and classes defined in this library are not part of drift's stable
/// API.
library;

import 'package:drift/drift.dart';

/// Signature of a function, typically generated by drift, that runs a single
/// migration step with a given [currentVersion] and the [database].
///
/// Returns the schema version code that the function migrates to.
typedef MigrationStepWithVersion = Future<int> Function(
  int currentVersion,
  GeneratedDatabase database,
);

/// A snapshot of a database schema at a previous version.
///
/// This class is meant to be extended by generated code.
abstract base class VersionedSchema {
  /// The generated database instance, used to create [TableInfo] instances.
  final DatabaseConnectionUser database;

  /// The [GeneratedDatabase.schemaVersion] at the time this schema was active.
  final int version;

  /// Default constructor taking the database and the schema version.
  VersionedSchema({required this.database, required this.version});

  /// All drift schema entities at the time of the set [version].
  Iterable<DatabaseSchemaEntity> get entities;

  /// A helper used by drift internally to implement the [step-by-step](https://drift.simonbinder.eu/docs/advanced-features/migrations/#step-by-step)
  /// migration feature.
  ///
  /// This method implements an [OnUpgrade] callback by repeatedly invoking
  /// [step] with the current version, assuming that [step] will perform an
  /// upgrade from that version to the version returned by the callback.
  ///
  /// If you want to customize the way the migration steps are invoked, for
  /// instance by running statements before and afterwards, see
  /// [runMigrationSteps].
  static OnUpgrade stepByStepHelper({
    required MigrationStepWithVersion step,
  }) {
    return (m, from, to) async {
      return await runMigrationSteps(
        migrator: m,
        from: from,
        to: to,
        steps: step,
      );
    };
  }

  /// Helper method that runs a (subset of) [stepByStepHelper] by invoking the
  /// [steps] function for each intermediate schema version from [from] until
  /// [to] is reached.
  ///
  /// This can be used to implement a custom `OnUpgrade` callback that runs
  /// additional checks before and after the migrations:
  ///
  /// ```dart
  /// onUpgrade: (m, from, to) async {
  ///  await customStatement('PRAGMA foreign_keys = OFF');
  ///
  ///  await transaction(
  ///    () => VersionedSchema.runMigrationSteps(
  ///      migrator: m,
  ///      from: from,
  ///      to: to,
  ///      steps: migrationSteps(
  ///        from1To2: ...,
  ///        ...
  ///      ),
  ///    ),
  ///  );
  ///
  ///  if (kDebugMode) {
  ///    final wrongForeignKeys = await customSelect('PRAGMA foreign_key_check').get();
  ///    assert(wrongForeignKeys.isEmpty, '${wrongForeignKeys.map((e) => e.data)}');
  ///  }
  ///
  ///  await customStatement('PRAGMA foreign_keys = ON;');
  /// },
  /// ```
  static Future<void> runMigrationSteps({
    required Migrator migrator,
    required int from,
    required int to,
    required MigrationStepWithVersion steps,
  }) async {
    final database = migrator.database;

    for (var target = from; target < to;) {
      final newVersion = await steps(target, database);
      assert(newVersion > target);

      // Saving the schema version after each step prevents the schema of the
      // database diverging from what's stored in `user_version` if a migration
      // fails halfway.
      // We can only reliably do this for sqlite3 at the moment since managing
      // schema versions happens at a lower layer and is not current exposed to
      // the query builder.
      if (database.executor.dialect == SqlDialect.sqlite) {
        await database.customStatement('pragma user_version = $newVersion');
      }

      target = newVersion;
    }
  }
}

/// A drift table implementation that, instead of being generated, is constructed
/// from individual fields
///
/// This allows the code generated for step-by-step migrations to be a lot
/// smaller than the code typically generated by drift. Features like type
/// converters or information about unique/primary keys are not present in these
/// tables.
class VersionedTable extends Table with TableInfo<Table, QueryRow> {
  @override
  final String entityName;
  final String? _alias;
  @override
  final bool isStrict;

  @override
  final bool withoutRowId;

  @override
  final DatabaseConnectionUser attachedDatabase;

  @override
  final List<GeneratedColumn> $columns;

  /// List of columns, represented as a function that returns the generated
  /// column when given the resolved table name.
  final List<GeneratedColumn Function(String)> _columnFactories;

  @override
  final List<String> customConstraints;

  /// Create a table from the individual fields.
  ///
  /// [columns] is a list of functions returning a [GeneratedColumn] when given
  /// the alias (or original name) of this table.
  VersionedTable({
    required this.entityName,
    required this.isStrict,
    required this.withoutRowId,
    required this.attachedDatabase,
    required List<GeneratedColumn Function(String)> columns,
    required List<String> tableConstraints,
    String? alias,
  })  : _columnFactories = columns,
        customConstraints = tableConstraints,
        $columns = [for (final column in columns) column(alias ?? entityName)],
        _alias = alias;

  /// Create a table by copying fields from [source] and applying an [alias].
  VersionedTable.aliased({
    required VersionedTable source,
    required String? alias,
  })  : entityName = source.entityName,
        isStrict = source.isStrict,
        withoutRowId = source.withoutRowId,
        attachedDatabase = source.attachedDatabase,
        customConstraints = source.customConstraints,
        _columnFactories = source._columnFactories,
        $columns = [
          for (final column in source._columnFactories)
            column(alias ?? source.entityName)
        ],
        _alias = alias;

  @override
  String get actualTableName => entityName;

  @override
  String get aliasedName => _alias ?? entityName;

  @override
  bool get dontWriteConstraints => true;

  @override
  QueryRow map(Map<String, dynamic> data, {String? tablePrefix}) {
    return QueryRow(data, attachedDatabase);
  }

  @override
  VersionedTable createAlias(String alias) {
    return VersionedTable.aliased(source: this, alias: alias);
  }
}

/// The version of [VersionedTable] for virtual tables.
class VersionedVirtualTable extends VersionedTable
    with VirtualTableInfo<Table, QueryRow> {
  @override
  final String moduleAndArgs;

  /// Create a small virtual table from the individual fields.
  VersionedVirtualTable({
    required super.entityName,
    required super.attachedDatabase,
    required super.columns,
    required this.moduleAndArgs,
    super.alias,
  }) : super(
          isStrict: false,
          withoutRowId: false,
          tableConstraints: [],
        );

  /// Create a virtual table by copying fields from [source] and applying a
  /// [alias] to columns.
  VersionedVirtualTable.aliased(
      {required VersionedVirtualTable source, required String? alias})
      : moduleAndArgs = source.moduleAndArgs,
        super.aliased(source: source, alias: alias);

  @override
  VersionedVirtualTable createAlias(String alias) {
    return VersionedVirtualTable.aliased(
      source: this,
      alias: alias,
    );
  }
}

/// A constructed from individual fields instead of being generated with a
/// dedicated class.
class VersionedView implements ViewInfo<HasResultSet, QueryRow>, HasResultSet {
  @override
  final String entityName;
  final String? _alias;

  @override
  final String createViewStmt;

  @override
  Map<SqlDialect, String>? get createViewStatements =>
      {SqlDialect.sqlite: createViewStmt};

  @override
  final List<GeneratedColumn> $columns;

  @override
  late final Map<String, GeneratedColumn> columnsByName = {
    for (final column in $columns) column.name: column,
  };

  /// List of columns, represented as a function that returns the generated
  /// column when given the resolved table name.
  final List<GeneratedColumn Function(String)> _columnFactories;

  @override
  final DatabaseConnectionUser attachedDatabase;

  /// Create a view from the individual fields on [ViewInfo].
  VersionedView({
    required this.entityName,
    required this.attachedDatabase,
    required this.createViewStmt,
    required List<GeneratedColumn Function(String)> columns,
    String? alias,
  })  : _columnFactories = columns,
        $columns = [for (final column in columns) column(alias ?? entityName)],
        _alias = alias;

  /// Copy an alias to a [source] view.
  VersionedView.aliased({required VersionedView source, required String? alias})
      : entityName = source.entityName,
        attachedDatabase = source.attachedDatabase,
        createViewStmt = source.createViewStmt,
        _columnFactories = source._columnFactories,
        $columns = [
          for (final column in source._columnFactories)
            column(alias ?? source.entityName)
        ],
        _alias = alias;

  @override
  String get aliasedName => _alias ?? entityName;

  @override
  HasResultSet get asDslTable => this;

  @override
  VersionedView createAlias(String alias) {
    return VersionedView.aliased(source: this, alias: alias);
  }

  @override
  QueryRow map(Map<String, dynamic> data, {String? tablePrefix}) {
    return QueryRow(data, attachedDatabase);
  }

  @override
  Query<HasResultSet, dynamic>? get query => null;

  @override
  Set<String> get readTables => const {};
}
