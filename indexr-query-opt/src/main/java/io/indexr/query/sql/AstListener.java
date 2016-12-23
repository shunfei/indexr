package io.indexr.query.sql;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.misc.NotNull;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.TerminalNode;

import io.indexr.query.parsers.RQLBaseListener;
import io.indexr.query.parsers.RQLParser;

public class AstListener extends RQLBaseListener {
    @Override
    public void enterGroupByList(@NotNull RQLParser.GroupByListContext ctx) {
        super.enterGroupByList(ctx);
    }

    @Override
    public void exitGroupByList(@NotNull RQLParser.GroupByListContext ctx) {
        super.exitGroupByList(ctx);
    }

    @Override
    public void enterWhereClause(@NotNull RQLParser.WhereClauseContext ctx) {
        super.enterWhereClause(ctx);
    }

    @Override
    public void exitWhereClause(@NotNull RQLParser.WhereClauseContext ctx) {
        super.exitWhereClause(ctx);
    }

    @Override
    public void enterPredicateParenthesisGroup(@NotNull RQLParser.PredicateParenthesisGroupContext ctx) {
        super.enterPredicateParenthesisGroup(ctx);
    }

    @Override
    public void exitPredicateParenthesisGroup(@NotNull RQLParser.PredicateParenthesisGroupContext ctx) {
        super.exitPredicateParenthesisGroup(ctx);
    }

    @Override
    public void enterIsClause(@NotNull RQLParser.IsClauseContext ctx) {
        super.enterIsClause(ctx);
    }

    @Override
    public void exitIsClause(@NotNull RQLParser.IsClauseContext ctx) {
        super.exitIsClause(ctx);
    }

    @Override
    public void enterBooleanOperator(@NotNull RQLParser.BooleanOperatorContext ctx) {
        super.enterBooleanOperator(ctx);
    }

    @Override
    public void exitBooleanOperator(@NotNull RQLParser.BooleanOperatorContext ctx) {
        super.exitBooleanOperator(ctx);
    }

    @Override
    public void enterComparisonClause(@NotNull RQLParser.ComparisonClauseContext ctx) {
        super.enterComparisonClause(ctx);
    }

    @Override
    public void exitComparisonClause(@NotNull RQLParser.ComparisonClauseContext ctx) {
        super.exitComparisonClause(ctx);
    }

    @Override
    public void enterTableName(@NotNull RQLParser.TableNameContext ctx) {
        super.enterTableName(ctx);
    }

    @Override
    public void exitTableName(@NotNull RQLParser.TableNameContext ctx) {
        super.exitTableName(ctx);
    }

    @Override
    public void enterLimitInteger(@NotNull RQLParser.LimitIntegerContext ctx) {
        super.enterLimitInteger(ctx);
    }

    @Override
    public void exitLimitInteger(@NotNull RQLParser.LimitIntegerContext ctx) {
        super.exitLimitInteger(ctx);
    }

    @Override
    public void enterExpressionParenthesisGroup(@NotNull RQLParser.ExpressionParenthesisGroupContext ctx) {
        super.enterExpressionParenthesisGroup(ctx);
    }

    @Override
    public void exitExpressionParenthesisGroup(@NotNull RQLParser.ExpressionParenthesisGroupContext ctx) {
        super.exitExpressionParenthesisGroup(ctx);
    }

    @Override
    public void enterIdentifier(@NotNull RQLParser.IdentifierContext ctx) {
        super.enterIdentifier(ctx);
    }

    @Override
    public void exitIdentifier(@NotNull RQLParser.IdentifierContext ctx) {
        super.exitIdentifier(ctx);
    }

    @Override
    public void enterPredicateList(@NotNull RQLParser.PredicateListContext ctx) {
        super.enterPredicateList(ctx);
    }

    @Override
    public void exitPredicateList(@NotNull RQLParser.PredicateListContext ctx) {
        super.exitPredicateList(ctx);
    }

    @Override
    public void enterStarExpression(@NotNull RQLParser.StarExpressionContext ctx) {
        super.enterStarExpression(ctx);
    }

    @Override
    public void exitStarExpression(@NotNull RQLParser.StarExpressionContext ctx) {
        super.exitStarExpression(ctx);
    }

    @Override
    public void enterFunction(@NotNull RQLParser.FunctionContext ctx) {
        super.enterFunction(ctx);
    }

    @Override
    public void exitFunction(@NotNull RQLParser.FunctionContext ctx) {
        super.exitFunction(ctx);
    }

    @Override
    public void enterGroupByClause(@NotNull RQLParser.GroupByClauseContext ctx) {
        super.enterGroupByClause(ctx);
    }

    @Override
    public void exitGroupByClause(@NotNull RQLParser.GroupByClauseContext ctx) {
        super.exitGroupByClause(ctx);
    }

    @Override
    public void enterStarColumnList(@NotNull RQLParser.StarColumnListContext ctx) {
        super.enterStarColumnList(ctx);
    }

    @Override
    public void exitStarColumnList(@NotNull RQLParser.StarColumnListContext ctx) {
        super.exitStarColumnList(ctx);
    }

    @Override
    public void enterOrderByClause(@NotNull RQLParser.OrderByClauseContext ctx) {
        super.enterOrderByClause(ctx);
    }

    @Override
    public void exitOrderByClause(@NotNull RQLParser.OrderByClauseContext ctx) {
        super.exitOrderByClause(ctx);
    }

    @Override
    public void enterIntegerLiteral(@NotNull RQLParser.IntegerLiteralContext ctx) {
        super.enterIntegerLiteral(ctx);
    }

    @Override
    public void exitIntegerLiteral(@NotNull RQLParser.IntegerLiteralContext ctx) {
        super.exitIntegerLiteral(ctx);
    }

    @Override
    public void enterGroupBy(@NotNull RQLParser.GroupByContext ctx) {
        super.enterGroupBy(ctx);
    }

    @Override
    public void exitGroupBy(@NotNull RQLParser.GroupByContext ctx) {
        super.exitGroupBy(ctx);
    }

    @Override
    public void enterBetweenPredicate(@NotNull RQLParser.BetweenPredicateContext ctx) {
        super.enterBetweenPredicate(ctx);
    }

    @Override
    public void exitBetweenPredicate(@NotNull RQLParser.BetweenPredicateContext ctx) {
        super.exitBetweenPredicate(ctx);
    }

    @Override
    public void enterComparisonOperator(@NotNull RQLParser.ComparisonOperatorContext ctx) {
        super.enterComparisonOperator(ctx);
    }

    @Override
    public void exitComparisonOperator(@NotNull RQLParser.ComparisonOperatorContext ctx) {
        super.exitComparisonOperator(ctx);
    }

    @Override
    public void enterBinaryMathOp(@NotNull RQLParser.BinaryMathOpContext ctx) {
        super.enterBinaryMathOp(ctx);
    }

    @Override
    public void exitBinaryMathOp(@NotNull RQLParser.BinaryMathOpContext ctx) {
        super.exitBinaryMathOp(ctx);
    }

    @Override
    public void enterOutputColumnList(@NotNull RQLParser.OutputColumnListContext ctx) {
        super.enterOutputColumnList(ctx);
    }

    @Override
    public void exitOutputColumnList(@NotNull RQLParser.OutputColumnListContext ctx) {
        super.exitOutputColumnList(ctx);
    }

    @Override
    public void enterInPredicate(@NotNull RQLParser.InPredicateContext ctx) {
        super.enterInPredicate(ctx);
    }

    @Override
    public void exitInPredicate(@NotNull RQLParser.InPredicateContext ctx) {
        super.exitInPredicate(ctx);
    }

    @Override
    public void enterBetweenClause(@NotNull RQLParser.BetweenClauseContext ctx) {
        super.enterBetweenClause(ctx);
    }

    @Override
    public void exitBetweenClause(@NotNull RQLParser.BetweenClauseContext ctx) {
        super.exitBetweenClause(ctx);
    }

    @Override
    public void enterHaving(@NotNull RQLParser.HavingContext ctx) {
        super.enterHaving(ctx);
    }

    @Override
    public void exitHaving(@NotNull RQLParser.HavingContext ctx) {
        super.exitHaving(ctx);
    }

    @Override
    public void enterTop(@NotNull RQLParser.TopContext ctx) {
        super.enterTop(ctx);
    }

    @Override
    public void exitTop(@NotNull RQLParser.TopContext ctx) {
        super.exitTop(ctx);
    }

    @Override
    public void enterStringLiteral(@NotNull RQLParser.StringLiteralContext ctx) {
        super.enterStringLiteral(ctx);
    }

    @Override
    public void exitStringLiteral(@NotNull RQLParser.StringLiteralContext ctx) {
        super.exitStringLiteral(ctx);
    }

    @Override
    public void enterTableNameSource(@NotNull RQLParser.TableNameSourceContext ctx) {
        super.enterTableNameSource(ctx);
    }

    @Override
    public void exitTableNameSource(@NotNull RQLParser.TableNameSourceContext ctx) {
        super.exitTableNameSource(ctx);
    }

    @Override
    public void enterOutputColumn(@NotNull RQLParser.OutputColumnContext ctx) {
        super.enterOutputColumn(ctx);
    }

    @Override
    public void exitOutputColumn(@NotNull RQLParser.OutputColumnContext ctx) {
        super.exitOutputColumn(ctx);
    }

    @Override
    public void enterTableNameAlias(@NotNull RQLParser.TableNameAliasContext ctx) {
        super.enterTableNameAlias(ctx);
    }

    @Override
    public void exitTableNameAlias(@NotNull RQLParser.TableNameAliasContext ctx) {
        super.exitTableNameAlias(ctx);
    }

    @Override
    public void enterWhere(@NotNull RQLParser.WhereContext ctx) {
        super.enterWhere(ctx);
    }

    @Override
    public void exitWhere(@NotNull RQLParser.WhereContext ctx) {
        super.exitWhere(ctx);
    }

    @Override
    public void enterIsPredicate(@NotNull RQLParser.IsPredicateContext ctx) {
        super.enterIsPredicate(ctx);
    }

    @Override
    public void exitIsPredicate(@NotNull RQLParser.IsPredicateContext ctx) {
        super.exitIsPredicate(ctx);
    }

    @Override
    public void enterTableNames(@NotNull RQLParser.TableNamesContext ctx) {
        super.enterTableNames(ctx);
    }

    @Override
    public void exitTableNames(@NotNull RQLParser.TableNamesContext ctx) {
        super.exitTableNames(ctx);
    }

    @Override
    public void enterConstant(@NotNull RQLParser.ConstantContext ctx) {
        super.enterConstant(ctx);
    }

    @Override
    public void exitConstant(@NotNull RQLParser.ConstantContext ctx) {
        super.exitConstant(ctx);
    }

    @Override
    public void enterOrdering(@NotNull RQLParser.OrderingContext ctx) {
        super.enterOrdering(ctx);
    }

    @Override
    public void exitOrdering(@NotNull RQLParser.OrderingContext ctx) {
        super.exitOrdering(ctx);
    }

    @Override
    public void enterInClause(@NotNull RQLParser.InClauseContext ctx) {
        super.enterInClause(ctx);
    }

    @Override
    public void exitInClause(@NotNull RQLParser.InClauseContext ctx) {
        super.exitInClause(ctx);
    }

    @Override
    public void enterPredicateExpr(@NotNull RQLParser.PredicateExprContext ctx) {
        super.enterPredicateExpr(ctx);
    }

    @Override
    public void exitPredicateExpr(@NotNull RQLParser.PredicateExprContext ctx) {
        super.exitPredicateExpr(ctx);
    }

    @Override
    public void enterComparisonPredicate(@NotNull RQLParser.ComparisonPredicateContext ctx) {
        super.enterComparisonPredicate(ctx);
    }

    @Override
    public void exitComparisonPredicate(@NotNull RQLParser.ComparisonPredicateContext ctx) {
        super.exitComparisonPredicate(ctx);
    }

    @Override
    public void enterRoot(@NotNull RQLParser.RootContext ctx) {
        super.enterRoot(ctx);
    }

    @Override
    public void exitRoot(@NotNull RQLParser.RootContext ctx) {
        super.exitRoot(ctx);
    }

    @Override
    public void enterStatement(@NotNull RQLParser.StatementContext ctx) {
        super.enterStatement(ctx);
    }

    @Override
    public void exitStatement(@NotNull RQLParser.StatementContext ctx) {
        super.exitStatement(ctx);
    }

    @Override
    public void enterFunctionCall(@NotNull RQLParser.FunctionCallContext ctx) {
        super.enterFunctionCall(ctx);
    }

    @Override
    public void exitFunctionCall(@NotNull RQLParser.FunctionCallContext ctx) {
        super.exitFunctionCall(ctx);
    }

    @Override
    public void enterBinaryMathOperator(@NotNull RQLParser.BinaryMathOperatorContext ctx) {
        super.enterBinaryMathOperator(ctx);
    }

    @Override
    public void exitBinaryMathOperator(@NotNull RQLParser.BinaryMathOperatorContext ctx) {
        super.exitBinaryMathOperator(ctx);
    }

    @Override
    public void enterExpressionList(@NotNull RQLParser.ExpressionListContext ctx) {
        super.enterExpressionList(ctx);
    }

    @Override
    public void exitExpressionList(@NotNull RQLParser.ExpressionListContext ctx) {
        super.exitExpressionList(ctx);
    }

    @Override
    public void enterOrderBy(@NotNull RQLParser.OrderByContext ctx) {
        super.enterOrderBy(ctx);
    }

    @Override
    public void exitOrderBy(@NotNull RQLParser.OrderByContext ctx) {
        super.exitOrderBy(ctx);
    }

    @Override
    public void enterOrderByList(@NotNull RQLParser.OrderByListContext ctx) {
        super.enterOrderByList(ctx);
    }

    @Override
    public void exitOrderByList(@NotNull RQLParser.OrderByListContext ctx) {
        super.exitOrderByList(ctx);
    }

    @Override
    public void enterOrderByExpression(@NotNull RQLParser.OrderByExpressionContext ctx) {
        super.enterOrderByExpression(ctx);
    }

    @Override
    public void exitOrderByExpression(@NotNull RQLParser.OrderByExpressionContext ctx) {
        super.exitOrderByExpression(ctx);
    }

    @Override
    public void enterHavingClause(@NotNull RQLParser.HavingClauseContext ctx) {
        super.enterHavingClause(ctx);
    }

    @Override
    public void exitHavingClause(@NotNull RQLParser.HavingClauseContext ctx) {
        super.exitHavingClause(ctx);
    }

    @Override
    public void enterLimitClause(@NotNull RQLParser.LimitClauseContext ctx) {
        super.enterLimitClause(ctx);
    }

    @Override
    public void exitLimitClause(@NotNull RQLParser.LimitClauseContext ctx) {
        super.exitLimitClause(ctx);
    }

    @Override
    public void enterColumnAliasName(@NotNull RQLParser.ColumnAliasNameContext ctx) {
        super.enterColumnAliasName(ctx);
    }

    @Override
    public void exitColumnAliasName(@NotNull RQLParser.ColumnAliasNameContext ctx) {
        super.exitColumnAliasName(ctx);
    }

    @Override
    public void enterFloatingPointLiteral(@NotNull RQLParser.FloatingPointLiteralContext ctx) {
        super.enterFloatingPointLiteral(ctx);
    }

    @Override
    public void exitFloatingPointLiteral(@NotNull RQLParser.FloatingPointLiteralContext ctx) {
        super.exitFloatingPointLiteral(ctx);
    }

    @Override
    public void enterTopClause(@NotNull RQLParser.TopClauseContext ctx) {
        super.enterTopClause(ctx);
    }

    @Override
    public void exitTopClause(@NotNull RQLParser.TopClauseContext ctx) {
        super.exitTopClause(ctx);
    }

    @Override
    public void enterSelect(@NotNull RQLParser.SelectContext ctx) {
        super.enterSelect(ctx);
    }

    @Override
    public void exitSelect(@NotNull RQLParser.SelectContext ctx) {
        super.exitSelect(ctx);
    }

    @Override
    public void enterLimit(@NotNull RQLParser.LimitContext ctx) {
        super.enterLimit(ctx);
    }

    @Override
    public void exitLimit(@NotNull RQLParser.LimitContext ctx) {
        super.exitLimit(ctx);
    }

    @Override
    public void enterEveryRule(@NotNull ParserRuleContext ctx) {
        super.enterEveryRule(ctx);
    }

    @Override
    public void exitEveryRule(@NotNull ParserRuleContext ctx) {
        super.exitEveryRule(ctx);
    }

    @Override
    public void visitTerminal(@NotNull TerminalNode node) {
        super.visitTerminal(node);
    }

    @Override
    public void visitErrorNode(@NotNull ErrorNode node) {
        super.visitErrorNode(node);
    }
}
