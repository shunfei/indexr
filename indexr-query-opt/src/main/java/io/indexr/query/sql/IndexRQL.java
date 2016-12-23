package io.indexr.query.sql;

import com.google.common.base.Preconditions;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.BailErrorStrategy;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonToken;
import org.antlr.v4.runtime.CommonTokenFactory;
import org.antlr.v4.runtime.RuleContext;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.UnbufferedTokenStream;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.RuleNode;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import io.indexr.query.expr.Cast;
import io.indexr.query.expr.Expression;
import io.indexr.query.expr.If;
import io.indexr.query.expr.Literal;
import io.indexr.query.expr.SortOrder;
import io.indexr.query.expr.agg.Average;
import io.indexr.query.expr.agg.Count;
import io.indexr.query.expr.agg.Max;
import io.indexr.query.expr.agg.Min;
import io.indexr.query.expr.agg.Sum;
import io.indexr.query.expr.arith.Add;
import io.indexr.query.expr.arith.BitwiseAnd;
import io.indexr.query.expr.arith.BitwiseOr;
import io.indexr.query.expr.arith.BitwiseXor;
import io.indexr.query.expr.arith.Divide;
import io.indexr.query.expr.arith.Multiply;
import io.indexr.query.expr.arith.Remainder;
import io.indexr.query.expr.arith.Subtract;
import io.indexr.query.expr.attr.NamedExpression;
import io.indexr.query.expr.attr.Star;
import io.indexr.query.expr.attr.UnresolvedAlias;
import io.indexr.query.expr.attr.UnresolvedAttribute;
import io.indexr.query.expr.predicate.And;
import io.indexr.query.expr.predicate.EqualTo;
import io.indexr.query.expr.predicate.GreaterThan;
import io.indexr.query.expr.predicate.GreaterThanOrEqual;
import io.indexr.query.expr.predicate.In;
import io.indexr.query.expr.predicate.LessThan;
import io.indexr.query.expr.predicate.LessThanOrEqual;
import io.indexr.query.expr.predicate.NotEqual;
import io.indexr.query.expr.predicate.Or;
import io.indexr.query.fn.One;
import io.indexr.query.parsers.RQLLexer;
import io.indexr.query.parsers.RQLParser;
import io.indexr.query.parsers.RQLParser.SelectStatementContext;
import io.indexr.query.parsers.RQLParser.StatementContext;
import io.indexr.query.plan.logical.Aggregate;
import io.indexr.query.plan.logical.Filter;
import io.indexr.query.plan.logical.Limit;
import io.indexr.query.plan.logical.LogicalPlan;
import io.indexr.query.plan.logical.OneRowRelation;
import io.indexr.query.plan.logical.Project;
import io.indexr.query.plan.logical.Sort;
import io.indexr.query.plan.logical.UnresolvedRelation;
import io.indexr.query.types.DataType;
import io.indexr.util.ExtraStringUtil;
import io.indexr.util.InstanceTypeAssigner;

import static io.indexr.util.Trick.mapToList;
import static io.indexr.util.Trick.one;

public class IndexRQL {
    private String sql;

    public IndexRQL(String sql) {
        this.sql = sql;
    }

    public LogicalPlan parseToPlan() {
        ParseTree root = parseSQL(sql);
        checkNode(root, RuleNode.class);
        if (root.getChildCount() == 0 || checkNode(root.getChild(0), RuleNode.class) == null) {
            return new OneRowRelation();
        }
        StatementContext statementContext = (StatementContext) root.getChild(0);
        return parseToSelectPlan(statementContext.selectStatement());
    }

    public class SelectSubClauses {
        public RQLParser.OutputColumnsContext outputColumns;
        public RQLParser.TableNameSourceContext tableNameSource;
        public RQLParser.WhereClauseContext whereClause;
        public RQLParser.GroupByClauseContext groupByClause;
        public RQLParser.HavingClauseContext havingClause;
        public RQLParser.OrderByClauseContext orderByClause;
        public RQLParser.LimitClauseContext limitClause;
    }

    private LogicalPlan parseToSelectPlan(SelectStatementContext selectClause) {
        SelectSubClauses subClauses = assignRuleContext(selectClause, new SelectSubClauses());
        LogicalPlan rootPlan;
        if (subClauses.tableNameSource == null) {
            rootPlan = new OneRowRelation();
        } else {
            String tableName = subClauses.tableNameSource.getText();
            rootPlan = new UnresolvedRelation(tableName);
        }
        List<NamedExpression> selectExpressions = mapToList(colProjectionToExprs(subClauses.outputColumns), e -> new UnresolvedAlias(e, null));
        if (subClauses.whereClause != null) {
            rootPlan = new Filter(
                    new Cast(parseExpr(subClauses.whereClause.predicateList()), DataType.BooleanType),
                    rootPlan);
        }
        if (subClauses.groupByClause != null) {
            rootPlan = new Aggregate(groupbyToExprs(subClauses.groupByClause),
                    selectExpressions,
                    rootPlan);
        } else {
            rootPlan = new Project(selectExpressions, rootPlan);
        }
        if (subClauses.havingClause != null) {
            rootPlan = new Filter(
                    new Cast(parseExpr(subClauses.havingClause.predicateList()), DataType.BooleanType),
                    rootPlan);
        }
        if (subClauses.orderByClause != null) {
            rootPlan = new Sort(parseOrderbys(subClauses.orderByClause), rootPlan);
        }
        if (subClauses.limitClause != null) {
            List<RQLParser.LimitIntegerContext> limits = subClauses.limitClause.limitInteger();
            if (limits.size() == 1) {
                rootPlan = new Limit(
                        new Literal(0, DataType.LongType),
                        new Cast(parseExpr(limits.get(0)), DataType.LongType),
                        rootPlan);
            } else {
                rootPlan = new Limit(
                        new Cast(parseExpr(limits.get(0)), DataType.LongType),
                        new Cast(parseExpr(limits.get(1)), DataType.LongType),
                        rootPlan);
            }
        } else {
            return rootPlan;
            // Default 1k.
            //rootPlan = new Limit(
            //        new Literal(0, DataType.LongType),
            //        new Literal(1024, DataType.LongType),
            //        rootPlan);
        }
        return rootPlan;
    }


    private static List<SortOrder> parseOrderbys(RQLParser.OrderByClauseContext orderByClause) {
        List<RQLParser.OrderByExpressionContext> orders = orderByClause.orderByList().orderByExpression();
        return orders.stream().map(o -> {
            if (o.ordering() == null || o.ordering().ASC() != null) {
                return new SortOrder(parseExpr(o.expression()), SortOrder.SortDirection.Ascending);
            } else {
                return new SortOrder(parseExpr(o.expression()), SortOrder.SortDirection.Descending);
            }
        }).collect(Collectors.toList());
    }

    private static List<Expression> colProjectionToExprs(RQLParser.OutputColumnsContext outputColumns) {
        if (outputColumns instanceof RQLParser.StarColumnListContext) {
            return Collections.singletonList(new Star());
        } else {
            RQLParser.OutputColumnListContext columnList = (RQLParser.OutputColumnListContext) outputColumns;
            ArrayList<Expression> expressions = new ArrayList<>(columnList.getChildCount());
            for (int i = 0; i < columnList.getChildCount(); i++) {
                RQLParser.OutputColumnProjectionContext outputColumn = columnList.outputColumnProjection(i);
                Expression expr = parseExpr(outputColumn);
                if (expr != null) {
                    expressions.add(expr);
                }
            }
            return expressions;
        }
    }

    private static List<Expression> groupbyToExprs(RQLParser.GroupByClauseContext groupByClause) {
        List<RQLParser.ExpressionContext> expressionContexts = groupByClause.groupByList().expression();
        return expressionContexts.stream().map(IndexRQL::parseExpr).collect(Collectors.toList());
    }

    private static Expression parseExpr(ParseTree node) {
        RuleContext exprCtx = checkNode(node, RuleContext.class);
        if (exprCtx == null) {
            return null;
        }
        if (exprCtx instanceof RQLParser.OutputColumnContext) {
            RQLParser.OutputColumnContext outputColumn = (RQLParser.OutputColumnContext) exprCtx;
            if (outputColumn.AS() == null) {
                return parseExpr(outputColumn.expression());
            } else {
                return new UnresolvedAlias(
                        parseExpr(outputColumn.expression()),
                        trimField(outputColumn.columnAliasName().getText()));
            }
        } else if (exprCtx instanceof RQLParser.ConstantContext) {
            return parseExpr(((RQLParser.ConstantContext) exprCtx).literal());
        } else if (exprCtx instanceof RQLParser.IntegerLiteralContext) {
            return new Literal(Long.parseLong(trimValue(exprCtx.getText())), DataType.LongType);
        } else if (exprCtx instanceof RQLParser.FloatingPointLiteralContext) {
            return new Literal(
                    Double.doubleToRawLongBits(Double.parseDouble(trimValue(exprCtx.getText()))),
                    DataType.DoubleType);
        } else if (exprCtx instanceof RQLParser.StringLiteralContext) {
            return new Literal(0, trimValue(exprCtx.getText()), DataType.StringType);
        } else if (exprCtx instanceof RQLParser.IdentifierContext) {
            String fieldName = trimField(exprCtx.getText());
            return new UnresolvedAttribute(fieldName);
        } else if (exprCtx instanceof RQLParser.BinaryMathOpContext) {
            ParseTree left = exprCtx.getChild(0);
            ParseTree right = exprCtx.getChild(2);
            String opStr = exprCtx.getChild(1).getText();
            switch (opStr) {
                case "+":
                    return new Add(parseExpr(left), parseExpr(right));
                case "-":
                    return new Subtract(parseExpr(left), parseExpr(right));
                case "*":
                    return new Multiply(parseExpr(left), parseExpr(right));
                case "/":
                    return new Divide(parseExpr(left), parseExpr(right));
                case "%":
                    return new Remainder(parseExpr(left), parseExpr(right));
                case "&":
                    return new BitwiseAnd(parseExpr(left), parseExpr(right));
                case "|":
                    return new BitwiseOr(parseExpr(left), parseExpr(right));
                case "^":
                    return new BitwiseXor(parseExpr(left), parseExpr(right));
                default:
                    throw new IllegalStateException(String.format("Unknown %s", opStr));
            }
        } else if (exprCtx instanceof RQLParser.PredicateExprContext) {
            RQLParser.PredicateExprContext predExpr = (RQLParser.PredicateExprContext) exprCtx;
            return new Cast(parseExpr(predExpr.predicate()), DataType.BooleanType);
        } else if (exprCtx instanceof RQLParser.FunctionCallContext) {
            RQLParser.FunctionCallContext functionCall = (RQLParser.FunctionCallContext) exprCtx;
            String functionName = trimField(functionCall.function().getText()).toUpperCase();
            RQLParser.ExpressionsContext expressions = functionCall.expressions();
            List<Expression> argExprs = new ArrayList<>();
            boolean isStarArg = false;
            if (expressions != null) {
                if (expressions instanceof RQLParser.StarExpressionContext) {
                    argExprs.add(new Star());
                    isStarArg = true;
                } else {
                    for (RQLParser.ExpressionContext arg : ((RQLParser.ExpressionListContext) expressions).expression()) {
                        Expression expr = parseExpr(arg);
                        if (expr != null) {
                            argExprs.add(expr);
                        }
                    }
                }
            }
            switch (functionName) {
                case "AVG":
                    Preconditions.checkState(argExprs.size() == 1);
                    return new Average(argExprs.get(0)).toAggregateExpression();
                case "COUNT":
                    if (isStarArg) {
                        // SELECT count(*) -> SELECT count(0)
                        return new Count(one(new Literal(0, DataType.LongType))).toAggregateExpression();
                    }
                    return new Count(argExprs).toAggregateExpression();
                case "MAX":
                    Preconditions.checkState(argExprs.size() == 1);
                    return new Max(argExprs.get(0)).toAggregateExpression();
                case "MIN":
                    Preconditions.checkState(argExprs.size() == 1);
                    return new Min(argExprs.get(0)).toAggregateExpression();
                case "SUM":
                    Preconditions.checkState(argExprs.size() == 1);
                    return new Sum(argExprs.get(0)).toAggregateExpression();
                case "CAST":
                case "CONVERT":
                    Preconditions.checkState(argExprs.size() == 2);
                    Expression typeExpr = argExprs.get(1);
                    DataType type;
                    if (typeExpr instanceof Literal) {
                        type = DataType.fromName(typeExpr.evalString(null).toString());
                    } else {
                        type = DataType.fromName(typeExpr.toString());
                    }
                    return new Cast(argExprs.get(0), type);
                case "IF":
                    Preconditions.checkState(argExprs.size() == 3);
                    return new If(argExprs.get(0), argExprs.get(1), argExprs.get(2));
                case "ONE":
                    Preconditions.checkState(argExprs.size() == 1);
                    return new One(argExprs.get(0));
                // TODO support UDF
                default:
                    throw new IllegalStateException(String.format("Unknown %s", functionName));
            }
        } else if (exprCtx instanceof RQLParser.PredicateListContext) {
            RQLParser.PredicateListContext predicateList = (RQLParser.PredicateListContext) exprCtx;
            List<RQLParser.PredicateContext> subPredicates = predicateList.predicate();
            List<RQLParser.BooleanOperatorContext> operators = predicateList.booleanOperator();
            Expression binaryRoot = new Cast(parseExpr(subPredicates.get(0)), DataType.BooleanType);
            for (int i = 0; i < operators.size(); i++) {
                String opName = trimField(operators.get(i).getText()).toUpperCase();
                Expression right = new Cast(parseExpr(subPredicates.get(i + 1)), DataType.BooleanType);
                switch (opName) {
                    case "AND":
                        binaryRoot = new And(binaryRoot, right);
                        break;
                    case "OR":
                        binaryRoot = new Or(binaryRoot, right);
                        break;
                    default:
                        throw new IllegalStateException(String.format("Unknown %s", opName));
                }
            }
            return binaryRoot;
        } else if (exprCtx instanceof RQLParser.PredicateParenthesisGroupContext) {
            RQLParser.PredicateParenthesisGroupContext parenthesisGroup = (RQLParser.PredicateParenthesisGroupContext) exprCtx;
            return parseExpr(parenthesisGroup.predicateList());
        } else if (exprCtx instanceof RQLParser.ComparisonPredicateContext) {
            RQLParser.ComparisonPredicateContext comparisonPredicate = (RQLParser.ComparisonPredicateContext) exprCtx;
            RQLParser.ComparisonClauseContext cmp = comparisonPredicate.comparisonClause();
            Preconditions.checkState(cmp.getChildCount() == 3);
            String cmpName = trimField(cmp.getChild(1).getText());
            Expression left = parseExpr(cmp.getChild(0));
            Expression right = parseExpr(cmp.getChild(2));
            switch (cmpName) {
                case "=":
                    return new EqualTo(left, right);
                case "!=":
                    return new NotEqual(left, right);
                case ">":
                    return new GreaterThan(left, right);
                case ">=":
                    return new GreaterThanOrEqual(left, right);
                case "<":
                    return new LessThan(left, right);
                case "<=":
                    return new LessThanOrEqual(left, right);
                default:
                    throw new IllegalStateException(String.format("Unknown %s", cmpName));
            }
        } else if (exprCtx instanceof RQLParser.BetweenPredicateContext) {
            // a between 10 and 20
            RQLParser.BetweenClauseContext betweenClause = ((RQLParser.BetweenPredicateContext) exprCtx).betweenClause();
            Preconditions.checkState(betweenClause.getChildCount() == 5);
            return new And(
                    new GreaterThanOrEqual(parseExpr(betweenClause.getChild(0)), parseExpr(betweenClause.getChild(2))),
                    new LessThanOrEqual(parseExpr(betweenClause.getChild(0)), parseExpr(betweenClause.getChild(4)))
            );
        } else if (exprCtx instanceof RQLParser.InPredicateContext) {
            RQLParser.InClauseContext inClause = ((RQLParser.InPredicateContext) exprCtx).inClause();
            Preconditions.checkState(inClause.getChildCount() >= 5);
            Expression valueExp = parseExpr(inClause.getChild(0));
            ArrayList<Expression> list = new ArrayList<>();
            for (int i = 3; i < inClause.getChildCount(); i += 2) {
                list.add(parseExpr(inClause.getChild(i)));
            }
            return new In(valueExp, list);
        } else if (exprCtx instanceof RQLParser.LimitIntegerContext) {
            return new Literal(Long.parseLong(exprCtx.getText()), DataType.LongType);
        } else if (exprCtx instanceof RQLParser.ExpressionParenthesisGroupContext) {
            return parseExpr(((RQLParser.ExpressionParenthesisGroupContext) exprCtx).expression());
        }
        throw new IllegalStateException(String.format("Unknown %s", exprCtx.getText()));
    }

    //private static Expression parsePredicate()

    private static String trimField(String name) {
        return ExtraStringUtil.trimAndSpace(name, "\"", "`");
    }

    private static String trimValue(String value) {
        return ExtraStringUtil.trim(value, "'");
    }

    private static <T> T checkNode(ParseTree node, Class<T> clazz) {
        if (node instanceof ErrorNode) {
            throw new IndexRQLParseError(node.getText());
        } else if (node instanceof TerminalNode) {
            return null;
        } else if (clazz.isInstance(node)) {
            return (T) node;
        }
        return null;
    }

    private static void parseTreeWalker(ParseTree node, Consumer<RuleNode> f) {
        RuleNode rule = checkNode(node, RuleNode.class);
        if (rule == null) {
            return;
        }
        f.accept(rule);
        for (int i = 0; i < rule.getChildCount(); i++) {
            parseTreeWalker(rule.getChild(i), f);
        }
    }

    private static <T> T assignRuleContext(ParseTree root, T ruleContainer) {
        InstanceTypeAssigner instanceTypeAssigner = new InstanceTypeAssigner(ruleContainer, RuleContext.class);
        parseTreeWalker(root, rule -> {
            instanceTypeAssigner.tryAssign(rule.getRuleContext());
        });
        return ruleContainer;
    }

    private static ParseTree parseSQL(String sql) {
        CharStream charStream = new ANTLRInputStream(sql);
        RQLLexer lexer = new RQLLexer(charStream);
        lexer.setTokenFactory(new CommonTokenFactory(true));
        TokenStream tokenStream = new UnbufferedTokenStream<CommonToken>(lexer);
        RQLParser parser = new RQLParser(tokenStream);
        parser.setErrorHandler(new BailErrorStrategy());
        return parser.root();
    }

    public static void main(String[] args) throws Exception {
        String sql = "select c0, sum(c1), max(c2), sum(c1) / sum(c4) as c1_c4 from A  where c0 > 10 and c1 != 0 group by c0 having c1_c4 > 0 order by sum(c1) limit 10, 20";

        //STRING sql = "select 10,a + 20, d, d-b+c as b_c, sum(d), sum(d+c) from AA  where a in(1,2,4) and c between 10 and 20 and a > 10 and (c in(1,2,3) or d = 2) group by a, b_c having sum(d) > 10 order by sum(d), sum(c) limit 10, 20";
        IndexRQL indexRQL = new IndexRQL(sql);
        LogicalPlan plan = indexRQL.parseToPlan();


        ObjectMapper om = new ObjectMapper();
        System.out.println(om.writeValueAsString(plan));
        System.out.println("=============");
        System.out.println(plan.toString());
    }
}
