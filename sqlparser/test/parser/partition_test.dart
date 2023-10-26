import 'package:sqlparser/sqlparser.dart';
import 'package:sqlparser/src/reader/parser.dart';
import 'package:sqlparser/src/reader/tokenizer/scanner.dart';
import 'package:sqlparser/src/utils/ast_equality.dart';
import 'package:test/test.dart';

import 'utils.dart';

final Map<String, Expression> _testCases = {
  'row_number() OVER (ORDER BY y)': WindowFunctionInvocation(
    function: identifier('row_number'),
    parameters: ExprFunctionParameters(),
    windowDefinition: WindowDefinition(
      frameSpec: FrameSpec(),
      orderBy: OrderBy(terms: [
        OrderingTerm(expression: Reference(columnName: 'y')),
      ]),
    ),
  ),
  'row_number(*) FILTER (WHERE 1) OVER '
          '(base_name PARTITION BY a, b '
          'GROUPS BETWEEN UNBOUNDED PRECEDING AND 3 FOLLOWING EXCLUDE TIES)':
      WindowFunctionInvocation(
    function: identifier('row_number'),
    parameters: StarFunctionParameter(),
    filter: NumericLiteral(1),
    windowDefinition: WindowDefinition(
      baseWindowName: 'base_name',
      partitionBy: [
        Reference(columnName: 'a'),
        Reference(columnName: 'b'),
      ],
      frameSpec: FrameSpec(
        type: FrameType.groups,
        start: FrameBoundary.unboundedPreceding(),
        end: FrameBoundary.following(
          NumericLiteral(3),
        ),
        excludeMode: ExcludeMode.ties,
      ),
    ),
  ),
  'row_number() OVER (RANGE CURRENT ROW EXCLUDE NO OTHERS)':
      WindowFunctionInvocation(
    function: identifier('row_number'),
    parameters: ExprFunctionParameters(),
    windowDefinition: WindowDefinition(
      frameSpec: FrameSpec(
        type: FrameType.range,
        start: FrameBoundary.currentRow(),
        end: FrameBoundary.currentRow(),
        excludeMode: ExcludeMode.noOthers,
      ),
    ),
  ),
  'COUNT(is_skipped) FILTER (WHERE is_skipped = true)':
      AggregateFunctionInvocation(
    function: identifier('COUNT'),
    parameters: ExprFunctionParameters(
      parameters: [Reference(columnName: 'is_skipped')],
    ),
    filter: BinaryExpression(
      Reference(columnName: 'is_skipped'),
      token(TokenType.equal),
      BooleanLiteral(true),
    ),
  ),
  "string_agg(foo, ', ' ORDER BY foo DESC)": AggregateFunctionInvocation(
    function: identifier('string_agg'),
    parameters: ExprFunctionParameters(parameters: [
      Reference(columnName: 'foo'),
      StringLiteral(', '),
    ]),
    orderBy: OrderBy(terms: [
      OrderingTerm(
        expression: Reference(columnName: 'foo'),
        orderingMode: OrderingMode.descending,
      ),
    ]),
  ),
};

void main() {
  group('partition parses', () {
    _testCases.forEach((sql, expected) {
      test(sql, () {
        final scanner = Scanner(sql);
        final tokens = scanner.scanTokens();
        final parser = Parser(tokens);
        final expression = parser.expression();

        enforceHasSpan(expression);
        enforceEqual(expression, expected);
      });
    });
  });
}
