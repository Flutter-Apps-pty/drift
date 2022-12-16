import 'package:collection/collection.dart';
import 'package:drift/drift.dart' show DriftSqlType;
import 'package:sqlparser/sqlparser.dart' as sql;

import 'dart.dart';
import 'element.dart';

import 'column.dart';
import 'result_sets.dart';

class DriftTable extends DriftElementWithResultSet {
  @override
  final List<DriftColumn> columns;

  final List<DriftTableConstraint> tableConstraints;

  @override
  final List<DriftElement> references;

  @override
  final ExistingRowClass? existingRowClass;

  @override
  final AnnotatedDartCode? customParentClass;

  /// The fixed [entityInfoName] to use, overriding the default.
  final String? fixedEntityInfoName;

  /// The default name to use for the [entityInfoName].
  final String baseDartName;

  @override
  final String nameOfRowClass;

  final bool withoutRowId;

  /// Information about the virtual table creating statement backing this table,
  /// if it [isVirtual].
  final VirtualTableData? virtualTableData;

  /// Whether this table is defined as `STRICT`. Support for strict tables has
  /// been added in sqlite 3.37.
  final bool strict;

  /// Whether the migrator should write SQL for [tableConstraints] added to this
  /// table (true by default).
  ///
  /// When disabled, only [overrideTableConstraints] entries will be written
  /// when creating the `CREATE TABLE` statement at runtime.
  final bool writeDefaultConstraints;

  /// When non-null, the generated table class will override the
  /// `customConstraints` getter in the table class with this value.
  final List<String> overrideTableConstraints;

  DriftTable(
    super.id,
    super.declaration, {
    required this.columns,
    required this.baseDartName,
    required this.nameOfRowClass,
    this.references = const [],
    this.existingRowClass,
    this.customParentClass,
    this.fixedEntityInfoName,
    this.withoutRowId = false,
    this.strict = false,
    this.tableConstraints = const [],
    this.virtualTableData,
    this.writeDefaultConstraints = true,
    this.overrideTableConstraints = const [],
  });

  /// Whether this is a virtual table, created with a `CREATE VIRTUAL TABLE`
  /// statement in SQL.
  bool get isVirtual => virtualTableData != null;

  @override
  String get dbGetterName => DriftSchemaElement.dbFieldName(baseDartName);

  /// The primary key for this table, computed by looking at the primary key
  /// defined as a table constraint or as a column constraint.
  Set<DriftColumn> get fullPrimaryKey {
    final fromTable =
        tableConstraints.whereType<PrimaryKeyColumns>().firstOrNull;

    if (fromTable != null) {
      return fromTable.primaryKey;
    }

    return columns
        .where((c) => c.constraints.any((f) => f is PrimaryKeyColumn))
        .toSet();
  }

  /// Determines whether [column] would be required for inserts performed via
  /// companions.
  bool isColumnRequiredForInsert(DriftColumn column) {
    assert(columns.contains(column));

    if (column.defaultArgument != null ||
        column.clientDefaultCode != null ||
        column.nullable ||
        column.isGenerated) {
      // default value would be applied, so it's not required for inserts
      return false;
    }

    // A column isn't required if it's an alias for the rowid, as explained
    // at https://www.sqlite.org/lang_createtable.html#rowid
    final fullPk = fullPrimaryKey;
    final isAliasForRowId = !withoutRowId &&
        column.sqlType == DriftSqlType.int &&
        fullPk.length == 1 &&
        fullPk.single == column;

    return !isAliasForRowId;
  }

  @override
  String get entityInfoName {
    // if this table was parsed from sql, a user might want to refer to it
    // directly because there is no user defined parent class.
    // So, turn CREATE TABLE users into something called "Users" instead of
    // "$UsersTable".
    final name =
        fixedEntityInfoName ?? _tableInfoNameForTableClass(baseDartName);
    if (name == nameOfRowClass) {
      // resolve clashes if the table info class has the same name as the data
      // class. This can happen because the data class name can be specified by
      // the user.
      return '${name}Table';
    }
    return name;
  }

  static String _tableInfoNameForTableClass(String className) =>
      '\$${className}Table';
}

abstract class DriftTableConstraint {}

class UniqueColumns extends DriftTableConstraint {
  final Set<DriftColumn> uniqueSet;

  UniqueColumns(this.uniqueSet);
}

class PrimaryKeyColumns extends DriftTableConstraint {
  final Set<DriftColumn> primaryKey;

  PrimaryKeyColumns(this.primaryKey);
}

class ForeignKeyTable extends DriftTableConstraint {
  final List<DriftColumn> localColumns;
  final DriftTable otherTable;

  /// The columns matching [localColumns] in the [otherTable].
  final List<DriftColumn> otherColumns;

  final sql.ReferenceAction? onUpdate;
  final sql.ReferenceAction? onDelete;

  ForeignKeyTable({
    required this.localColumns,
    required this.otherTable,
    required this.otherColumns,
    this.onUpdate,
    this.onDelete,
  });
}

class VirtualTableData {
  /// The module used to create this table.
  ///
  /// In `CREATE VIRTUAL TABLE foo USING fts5`, the [module] would be `fts5`.
  final String module;

  /// The argument content immmediately following the [module] in the creating
  /// statement.
  final List<String> moduleArguments;

  final RecognizedVirtualTableModule? recognized;

  VirtualTableData(this.module, this.moduleArguments, this.recognized);
}

abstract class RecognizedVirtualTableModule {}

class DriftFts5Table extends RecognizedVirtualTableModule {
  /// For fts5 tables with external content (https://www.sqlite.org/fts5.html#external_content_tables),
  /// references the drift table providing the content.
  final DriftTable? externalContentTable;

  /// If this fts5 table has an [externalContentTable] and uses an explicit
  /// column as a rowid, this is a reference to that column.
  final DriftColumn? externalContentRowId;

  DriftFts5Table(this.externalContentTable, this.externalContentRowId)
      : assert(externalContentRowId == null ||
            externalContentRowId.owner == externalContentTable);
}
