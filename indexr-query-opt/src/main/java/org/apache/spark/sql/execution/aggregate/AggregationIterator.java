package org.apache.spark.sql.execution.aggregate;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import io.indexr.query.expr.Expression;
import io.indexr.query.expr.agg.AggregateExpression;
import io.indexr.query.expr.agg.AggregateFunction;
import io.indexr.query.expr.agg.AggregateMode;
import io.indexr.query.expr.attr.Attribute;
import io.indexr.query.expr.attr.NamedExpression;
import io.indexr.query.expr.project.MutableProjection;
import io.indexr.query.expr.project.UnsafeProjection;
import io.indexr.query.row.InternalRow;
import io.indexr.query.row.JoinedRow;
import io.indexr.query.row.MutableRow;
import io.indexr.query.row.UnsafeRow;

import static io.indexr.util.Trick.concatToList;
import static io.indexr.util.Trick.flatMapToList;
import static io.indexr.util.Trick.mapToList;

/**
 * The base class of [[SortBasedAggregationIterator]] and [[TungstenAggregationIterator]].
 * It mainly contains two parts:
 * 1. It initializes aggregate functions.
 * 2. It creates two functions, `processRow` and `generateOutput` based on [[AggregateMode]] of
 * its aggregate functions. `processRow` is the function to handle an input. `generateOutput`
 * is used to generate result.
 */
public abstract class AggregationIterator implements Iterator<UnsafeRow> {
    protected List<NamedExpression> groupingExpressions;
    protected List<Attribute> inputAttributes;
    protected List<AggregateExpression> aggregateExpressions;
    protected List<Attribute> aggregateAttributes;
    protected int initialInputBufferOffset;
    protected List<NamedExpression> resultExpressions;
    protected BiFunction<List<Expression>, List<Attribute>, Supplier<MutableProjection>> newMutableProjection;

    protected Set<AggregateMode> modes;

    protected List<AggregateFunction> aggregateFunctions;
    protected MutableProjection expressionAggInitialProjection;
    protected ProcessRow processRow;
    protected UnsafeProjection groupingProjection;
    protected List<Attribute> groupingAttributes;
    protected GenerateOutput generateOutput;

    public AggregationIterator(
            List<NamedExpression> groupingExpressions,
            List<Attribute> inputAttributes,
            List<AggregateExpression> aggregateExpressions,
            List<Attribute> aggregateAttributes,
            int initialInputBufferOffset,
            List<NamedExpression> resultExpressions,
            BiFunction<List<Expression>, List<Attribute>, Supplier<MutableProjection>> newMutableProjection) {
        this.groupingExpressions = groupingExpressions;
        this.inputAttributes = inputAttributes;
        this.aggregateExpressions = aggregateExpressions;
        this.aggregateAttributes = aggregateAttributes;
        this.initialInputBufferOffset = initialInputBufferOffset;
        this.resultExpressions = resultExpressions;
        this.newMutableProjection = newMutableProjection;
        this.modes = aggregateExpressions.stream().map(e -> e.mode).distinct().collect(Collectors.toSet());

        checkArgs();

        this.aggregateFunctions = initializeAggregateFunctions(aggregateExpressions);
        this.expressionAggInitialProjection = newMutableProjection.apply(
                flatMapToList(aggregateFunctions, AggregateFunction::initialValues),
                null
        ).get();
        this.processRow = generateProcessRow(aggregateExpressions, aggregateFunctions, inputAttributes);
        this.groupingProjection = UnsafeProjection.create(groupingExpressions, inputAttributes);
        this.groupingAttributes = mapToList(groupingExpressions, NamedExpression::toAttribute);
        this.generateOutput = generateResultProjection();
    }

    private void checkArgs() {
        // At most two modes, and must either be sub set of [Partial, PartialMerge] or [Final, Complete].
        Preconditions.checkState(modes.size() <= 2);
        Preconditions.checkState(
                Sets.difference(modes, Sets.newHashSet(AggregateMode.Partial, AggregateMode.PartialMerge)).isEmpty()
                        || Sets.difference(modes, Sets.newHashSet(AggregateMode.Final, AggregateMode.Complete)).isEmpty());
    }

    protected List<AggregateFunction> initializeAggregateFunctions(
            List<AggregateExpression> expressions) {
        return mapToList(expressions, e -> e.aggregateFunction);
    }

    // Initialize all AggregateFunctions by binding references if necessary,
    // and set inputBufferOffset and mutableBufferOffset.
    protected ProcessRow generateProcessRow(
            List<AggregateExpression> expressions,
            List<AggregateFunction> functions,
            List<Attribute> inputAttributes) {
        if (expressions.isEmpty()) {
            // Grouping only.
            return (a, b) -> {
            };
        } else {
            List<Expression> processExpressions = new ArrayList<>();
            for (int i = 0; i < expressions.size(); i++) {
                switch (expressions.get(i).mode) {
                    case Partial:
                    case Complete:
                        processExpressions.addAll(functions.get(i).updateExpressions());
                        break;
                    case PartialMerge:
                    case Final:
                        processExpressions.addAll(functions.get(i).mergeExpressions());
                }
            }
            List<Attribute> aggregationBufferSchema = flatMapToList(functions, AggregateFunction::aggBufferAttributes);
            MutableProjection updateProjection = newMutableProjection.apply(
                    processExpressions,
                    concatToList(aggregationBufferSchema, inputAttributes)).get();
            JoinedRow joinedRow = new JoinedRow();
            return (currentBuffer, row) -> {
                updateProjection.target(currentBuffer).apply(joinedRow.apply(currentBuffer, row));
            };
        }
    }

    // Initializing the function used to generate the output row.
    protected GenerateOutput generateResultProjection() {
        JoinedRow joinedRow = new JoinedRow();
        List<Attribute> bufferAttributes = flatMapToList(aggregateFunctions, AggregateFunction::aggBufferAttributes);
        if (modes.contains(AggregateMode.Final) || modes.contains(AggregateMode.Complete)) {
            List<Expression> evalExpressions = mapToList(aggregateFunctions, AggregateFunction::evaluateExpression);
            UnsafeRow aggregateResult = UnsafeRow.create8BytesFieldsRow(aggregateAttributes.size());

            MutableProjection expressionAggEvalProjection = newMutableProjection.apply(evalExpressions, bufferAttributes).get();
            expressionAggEvalProjection.target(aggregateResult);

            UnsafeProjection resultProjection = UnsafeProjection.create(
                    resultExpressions,
                    concatToList(groupingAttributes, aggregateAttributes));

            return (currentGroupingKey, currentBuffer) -> {
                expressionAggEvalProjection.apply(currentBuffer);
                return resultProjection.apply(joinedRow.apply(currentGroupingKey, aggregateResult));
            };
        } else if (modes.contains(AggregateMode.Partial) || modes.contains(AggregateMode.PartialMerge)) {
            UnsafeProjection resultProjection = UnsafeProjection.create(
                    concatToList(groupingAttributes, bufferAttributes),
                    concatToList(groupingAttributes, bufferAttributes)
            );
            return (currentGroupingKey, currentBuffer) -> {
                return resultProjection.apply(joinedRow.apply(currentGroupingKey, currentBuffer));
            };
        } else {
            // Grouping-only: we only output values of grouping expressions.
            UnsafeProjection resultProjection = UnsafeProjection.create(resultExpressions, groupingAttributes);
            return (currentGroupingKey, currentBuffer) -> {
                return resultProjection.apply(currentGroupingKey);
            };
        }
    }


    /** Initializes buffer values for all aggregate functions. */
    protected void initializeBuffer(MutableRow buffer) {
        expressionAggInitialProjection.target(buffer).apply(InternalRow.EmptyRow);
    }

    @FunctionalInterface
    protected interface ProcessRow {
        void apply(MutableRow buffer, InternalRow row);
    }

    @FunctionalInterface
    protected interface GenerateOutput {
        UnsafeRow apply(UnsafeRow groupingKey, MutableRow buffer);
    }
}
