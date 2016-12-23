package io.indexr.query.expr.agg;

public enum AggregateMode {
    /**
     * An [[AggregateFunction]] with [[Partial]] mode is used for partial aggregation.
     * This function updates the given aggregation buffer with the original input of this
     * function. When it has processed all input rows, the aggregation buffer is returned.
     */
    Partial,
    /**
     * An [[AggregateFunction]] with [[PartialMerge]] mode is used to merge aggregation buffers
     * containing intermediate results for this function.
     * This function updates the given aggregation buffer by merging multiple aggregation buffers.
     * When it has processed all input rows, the aggregation buffer is returned.
     */
    PartialMerge,
    /**
     * An [[AggregateFunction]] with [[Final]] mode is used to merge aggregation buffers
     * containing intermediate results for this function and then generate final result.
     * This function updates the given aggregation buffer by merging multiple aggregation buffers.
     * When it has processed all input rows, the final result of this function is returned.
     */
    Final,
    /**
     * An [[AggregateFunction]] with [[Complete]] mode is used to evaluate this function directly
     * from original input rows without any partial aggregation.
     * This function updates the given aggregation buffer with the original input of this
     * function. When it has processed all input rows, the final result of this function is returned.
     */
    Complete;
}
