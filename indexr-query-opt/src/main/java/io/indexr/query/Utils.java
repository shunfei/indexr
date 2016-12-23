package io.indexr.query;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import io.indexr.query.expr.agg.AggregateExpression;
import io.indexr.query.expr.agg.AggregateFunction;
import io.indexr.query.expr.agg.AggregateMode;
import io.indexr.query.expr.attr.Attribute;
import io.indexr.query.expr.attr.AttributeReference;
import io.indexr.query.expr.attr.NamedExpression;
import io.indexr.query.plan.physical.PhysicalPlan;
import io.indexr.query.plan.physical.TungstenAggregate;

import static io.indexr.util.Trick.concatToList;
import static io.indexr.util.Trick.flatMapToList;
import static io.indexr.util.Trick.mapToList;
import static io.indexr.util.Trick.up;

public class Utils {


    public static List<PhysicalPlan> planAggregateWithoutDistinct(
            List<NamedExpression> groupingExpressions,
            List<AggregateExpression> aggregateExpressions,
            Map<AggregateFunction, Attribute> aggregateFunctionToAttribute,
            List<NamedExpression> resultExpressions,
            PhysicalPlan child) {
        // Check if we can use TungstenAggregate.

        // 1. Create an Aggregate Operator for partial aggregations.
        List<Attribute> groupingAttributes = mapToList(groupingExpressions, NamedExpression::toAttribute);
        List<AggregateExpression> partialAggregateExpressions = mapToList(
                aggregateExpressions,
                e -> new AggregateExpression(e.aggregateFunction, AggregateMode.Partial));
        List<AttributeReference> partialAggregateAttributes = flatMapToList(
                partialAggregateExpressions,
                e -> e.aggregateFunction.aggBufferAttributes());
        List<Attribute> partialResultExpressions = concatToList(
                groupingAttributes,
                flatMapToList(partialAggregateExpressions, e -> {
                    return e.aggregateFunction.inputAggBufferAttributes();
                }));

        PhysicalPlan partialAggregate = createAggregate(
                groupingExpressions,
                partialAggregateExpressions,
                up(partialAggregateAttributes),
                0,
                up(partialResultExpressions),
                child);


        // 2. Create an Aggregate Operator for final aggregations.
        List<AggregateExpression> finalAggregateExpressions = mapToList(
                aggregateExpressions,
                e -> new AggregateExpression(e.aggregateFunction, AggregateMode.Final));
        // The attributes of the final aggregation buffer, which is presented as input to the result
        // projection:
        List<Attribute> finalAggregateAttributes = mapToList(
                finalAggregateExpressions,
                e -> aggregateFunctionToAttribute.get(e.aggregateFunction));

        PhysicalPlan finalAggregate = createAggregate(
                up(groupingAttributes),
                finalAggregateExpressions,
                finalAggregateAttributes,
                groupingExpressions.size(),
                resultExpressions,
                partialAggregate);

        return Collections.singletonList(finalAggregate);
    }

    private static PhysicalPlan createAggregate(
            List<NamedExpression> groupingExpressions,
            List<AggregateExpression> aggregateExpressions,
            List<Attribute> aggregateAttributes,
            int initialInputBufferOffset,
            List<NamedExpression> resultExpressions,
            PhysicalPlan child) {
        return new TungstenAggregate(
                groupingExpressions,
                aggregateExpressions,
                aggregateAttributes,
                initialInputBufferOffset,
                resultExpressions,
                child);

    }
}
