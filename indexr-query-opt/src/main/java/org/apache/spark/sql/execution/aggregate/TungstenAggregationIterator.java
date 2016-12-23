package org.apache.spark.sql.execution.aggregate;

import org.apache.spark.sql.execution.UnsafeFixedWidthAggregationMap;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.KVIterator;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import io.indexr.query.TaskContext;
import io.indexr.query.expr.Expression;
import io.indexr.query.expr.agg.AggregateExpression;
import io.indexr.query.expr.agg.AggregateFunction;
import io.indexr.query.expr.attr.Attribute;
import io.indexr.query.expr.attr.NamedExpression;
import io.indexr.query.expr.project.MutableProjection;
import io.indexr.query.row.InternalRow;
import io.indexr.query.row.UnsafeRow;

import static io.indexr.util.Trick.flatMapToList;
import static io.indexr.util.Trick.mapToList;


public class TungstenAggregationIterator extends AggregationIterator {
    private Iterator<InternalRow> inputIter;

    public TungstenAggregationIterator(
            List<NamedExpression> groupingExpressions,
            List<AggregateExpression> aggregateExpressions,
            List<Attribute> aggregateAttributes,
            int initialInputBufferOffset,
            List<NamedExpression> resultExpressions,
            BiFunction<List<Expression>, List<Attribute>, Supplier<MutableProjection>> newMutableProjection,
            List<Attribute> originalInputAttributes,
            Iterator<InternalRow> inputIter
    ) {
        super(groupingExpressions,
                originalInputAttributes,
                aggregateExpressions,
                aggregateAttributes,
                initialInputBufferOffset,
                resultExpressions,
                newMutableProjection);
        this.inputIter = inputIter;

        // Start processing input rows.
        processInputs();
        // First, set aggregationBufferMapIterator.
        aggregationBufferMapIterator = hashMap.iterator();
        // Pre-load the first key-value pair from the aggregationBufferMapIterator.
        try {
            mapIteratorHasNext = aggregationBufferMapIterator.next();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        // If the map is empty, we just free it.
        if (!mapIteratorHasNext) {
            hashMap.free();
        }
    }

    private UnsafeRow createNewAggregationBuffer() {
        UnsafeRow buffer = UnsafeRow.create8BytesFieldsRow(aggregateFunctions.size());
        expressionAggInitialProjection.target(buffer).apply(InternalRow.EmptyRow);
        return buffer;
    }

    private UnsafeRow initialAggregationBuffer = createNewAggregationBuffer();

    // This is the hash map used for hash-based aggregation. It is backed by an
    // UnsafeFixedWidthAggregationMap and it is used to store
    // all groups and their corresponding aggregation buffers for hash-based aggregation.
    private UnsafeFixedWidthAggregationMap hashMap = new UnsafeFixedWidthAggregationMap(
            initialAggregationBuffer,
            StructType.fromAttributes(flatMapToList(aggregateFunctions, AggregateFunction::aggBufferAttributes)),
            StructType.fromAttributes(mapToList(groupingExpressions, NamedExpression::toAttribute)),
            TaskContext.get().taskMemoryManager(),
            1024 * 16, // initial capacity
            TaskContext.get().taskMemoryManager().pageSizeBytes(),
            false // disable tracking of performance metrics
    );

    // The function used to read and process input rows. When processing input rows,
    // it first uses hash-based aggregation by putting groups and their buffers in
    // hashMap. If there is not enough memory, it will multiple hash-maps, spilling
    // after each becomes full then using sort to merge these spills, finally do sort
    // based aggregation.
    private void processInputs() {
        if (groupingExpressions.isEmpty()) {
            // If there is no grouping expressions, we can just reuse the same buffer over and over again.
            // Note that it would be better to eliminate the hash map entirely in the future.
            UnsafeRow groupingKey = groupingProjection.apply(null);
            UnsafeRow buffer = hashMap.getAggregationBufferFromUnsafeRow(groupingKey);
            while (inputIter.hasNext()) {
                InternalRow newInput = inputIter.next();
                processRow.apply(buffer, newInput);
            }
        } else {
            while (inputIter.hasNext()) {
                InternalRow newInput = inputIter.next();
                UnsafeRow groupingKey = groupingProjection.apply(newInput);
                if (groupingKey.hasString()) {
                    throw new RuntimeException("Grouping keys with string is not yet supported");
                }
                UnsafeRow buffer = hashMap.getAggregationBufferFromUnsafeRow(groupingKey);
                if (buffer == null) {
                    throw new RuntimeException("No enough memory for aggregation");
                }
                processRow.apply(buffer, newInput);
            }
        }
    }

    // The iterator created from hashMap. It is used to generate output rows when we
    // are using hash-based aggregation.
    private KVIterator<UnsafeRow, UnsafeRow> aggregationBufferMapIterator;

    // Indicates if aggregationBufferMapIterator still has key-value pairs.
    private boolean mapIteratorHasNext = false;

    // The aggregation buffer used by the sort-based aggregation.
    private UnsafeRow sortBasedAggregationBuffer = createNewAggregationBuffer();

    @Override
    public boolean hasNext() {
        return mapIteratorHasNext;
    }

    @Override
    public UnsafeRow next() {
        if (hasNext()) {
            UnsafeRow result = generateOutput.apply(
                    aggregationBufferMapIterator.getKey(),
                    aggregationBufferMapIterator.getValue());
            try {
                // Pre-load next key-value pair form aggregationBufferMapIterator to make hasNext
                // idempotent.
                mapIteratorHasNext = aggregationBufferMapIterator.next();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            if (!mapIteratorHasNext) {
                // If there is no input from aggregationBufferMapIterator, we copy current result.
                UnsafeRow resultCopy = result.copy();
                // Then, we free the map.
                hashMap.free();
                return resultCopy;
            } else {
                return result;
            }
        } else {
            // no more result
            throw new NoSuchElementException();
        }
    }

    /**
     * Generate a output row when there is no input and there is no grouping expression.
     */
    public UnsafeRow outputForEmptyGroupingKeyWithoutInput() {
        if (groupingExpressions.isEmpty()) {
            // We create a output row and copy it. So, we can free the map.
            UnsafeRow resultCopy = generateOutput.apply(UnsafeRow.createFromByteArray(0, 0), initialAggregationBuffer).copy();
            hashMap.free();
            return resultCopy;
        } else {
            throw new IllegalStateException("This method should not be called when groupingExpressions is not empty.");
        }
    }
}
