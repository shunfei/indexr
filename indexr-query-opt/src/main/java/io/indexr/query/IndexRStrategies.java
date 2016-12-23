package io.indexr.query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.indexr.query.expr.Expression;
import io.indexr.query.expr.agg.AggregateExpression;
import io.indexr.query.expr.agg.AggregateFunction;
import io.indexr.query.expr.attr.Alias;
import io.indexr.query.expr.attr.Attribute;
import io.indexr.query.expr.attr.NamedExpression;
import io.indexr.query.plan.logical.Aggregate;
import io.indexr.query.plan.logical.Filter;
import io.indexr.query.plan.logical.Limit;
import io.indexr.query.plan.logical.LogicalPlan;
import io.indexr.query.plan.logical.NoOp;
import io.indexr.query.plan.logical.OneRowRelation;
import io.indexr.query.plan.logical.Project;
import io.indexr.query.plan.logical.Sort;
import io.indexr.query.plan.physical.LocalTableScan;
import io.indexr.query.plan.physical.PhysicalPlan;
import io.indexr.query.row.InternalRow;

import static io.indexr.util.Trick.flatMapToList;
import static io.indexr.util.Trick.mapToList;
import static io.indexr.util.Trick.one;

public abstract class IndexRStrategies extends QueryPlanner<PhysicalPlan> {

    class Aggregation implements Strategy {
        @Override
        public List apply(LogicalPlan plan) {
            if (!(plan instanceof Aggregate)) {
                return Collections.emptyList();
            }
            Aggregate aggregate = (Aggregate) plan;
            List<Expression> groupingExpressions = aggregate.groupingExpressions;
            List<NamedExpression> resultExpressions = aggregate.aggregateExpressions;
            LogicalPlan child = aggregate.child();

            // A single aggregate expression might appear multiple times in resultExpressions.
            // In order to avoid evaluating an individual aggregate function multiple times, we'll
            // build a set of the distinct aggregate expressions and build a function which can
            // be used to re-write expressions so that they reference the single copy of the
            // aggregate function which actually gets computed.
            List<AggregateExpression> aggregateExpressions = flatMapToList(resultExpressions, e -> {
                return e.collect(agg -> agg instanceof AggregateExpression ? (AggregateExpression) agg : null);
            }).stream().distinct().collect(Collectors.toList());

            // For those distinct aggregate expressions, we create a map from the
            // aggregate function to the corresponding attribute of the function.
            Map<AggregateFunction, Attribute> aggregateFunctionToAttribute = new HashMap<>();
            for (AggregateExpression agg : aggregateExpressions) {
                aggregateFunctionToAttribute.put(
                        agg.aggregateFunction,
                        new Alias(agg.aggregateFunction, agg.aggregateFunction.toString()).toAttribute());
            }

            Map<Expression, NamedExpression> groupExpressionMap = new HashMap<>();
            List<NamedExpression> namedGroupingExpressions = new ArrayList<>();
            for (Expression e : groupingExpressions) {
                if (e instanceof NamedExpression) {
                    namedGroupingExpressions.add((NamedExpression) e);
                    groupExpressionMap.put(e, (NamedExpression) e);
                } else {
                    // If the expression is not a NamedExpressions, we add an alias.
                    // So, when we generate the result of the operator, the Aggregate Operator
                    // can directly get the Seq of attributes representing the grouping expressions.
                    Alias withAlias = new Alias(e, e.toString());
                    namedGroupingExpressions.add(withAlias);
                    groupExpressionMap.put(e, withAlias);
                }
            }

            // The original `resultExpressions` are a set of expressions which may reference
            // aggregate expressions, grouping column values, and constants. When aggregate operator
            // emits output rows, we will use `resultExpressions` to generate an output projection
            // which takes the grouping columns and final aggregate result buffer as input.
            // Thus, we must re-write the result expressions so that their attributes match up with
            // the attributes of the final result projection's input row:
            List<NamedExpression> rewrittenResultExpressions = mapToList(resultExpressions, expr -> {
                        return (NamedExpression) expr.transformDown(e -> {
                            if (e instanceof AggregateExpression) {
                                // The final aggregation buffer's attributes will be `finalAggregationAttributes`,
                                // so replace each aggregate expression by its corresponding attribute in the set:
                                AggregateExpression agg = (AggregateExpression) e;
                                return aggregateFunctionToAttribute.get(agg.aggregateFunction);
                            } else {
                                // Since we're using `namedGroupingAttributes` to extract the grouping key
                                // columns, we need to replace grouping key expressions with their corresponding
                                // attributes. We do not rely on the equality check at here since attributes may
                                // differ cosmetically. Instead, we use semanticEquals.
                                for (Map.Entry<Expression, NamedExpression> entry : groupExpressionMap.entrySet()) {
                                    Expression expression = entry.getKey();
                                    NamedExpression ne = entry.getValue();
                                    if (expression.semanticEquals(e)) {
                                        return ne.toAttribute();
                                    }
                                }
                            }
                            return e;
                        });
                    }
            );

            return Utils.planAggregateWithoutDistinct(
                    namedGroupingExpressions,
                    aggregateExpressions,
                    aggregateFunctionToAttribute,
                    rewrittenResultExpressions,
                    planLater(child));
        }
    }

    class BasicOperators implements Strategy {
        @Override
        public List apply(LogicalPlan plan) {
            if (plan instanceof Sort) {
                Sort sort = (Sort) plan;
                return one(new io.indexr.query.plan.physical.Sort(
                        sort.order, planLater(sort.child())));
            } else if (plan instanceof Limit) {
                Limit limit = (Limit) plan;
                return one(new io.indexr.query.plan.physical.Limit(
                        limit.offsetExpr.evalLong(InternalRow.EmptyRow),
                        limit.limitExpr.evalLong(InternalRow.EmptyRow),
                        planLater(limit.child())
                ));
            } else if (plan instanceof Filter) {
                Filter filter = (Filter) plan;
                return one(new io.indexr.query.plan.physical.Filter(
                        filter.condition,
                        planLater(filter.child())
                ));
            } else if (plan instanceof Project) {
                Project project = (Project) plan;
                return one(new io.indexr.query.plan.physical.Project(
                        project.projectList,
                        planLater(project.child())
                ));
            } else if (plan instanceof NoOp) {
                NoOp noOp = (NoOp) plan;
                return one(planLater(noOp.child()));
            } else if (plan instanceof OneRowRelation) {
                OneRowRelation oneRow = (OneRowRelation) plan;
                return one(new LocalTableScan(
                        Collections.emptyList(),
                        one(InternalRow.SingleRow)));
            }
            //else if (plan instanceof LogicalRelation) {
            //    LogicalRelation relation = (LogicalRelation) plan;
            //    return one(new SegmentFilteredScan(relation.output(), relation.segments));
            //}
            return Collections.emptyList();
        }
    }
}
