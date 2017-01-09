package io.indexr.segment.rc;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.io.IOException;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import io.indexr.segment.ColumnSchema;
import io.indexr.segment.InfoSegment;
import io.indexr.segment.Segment;
import io.indexr.segment.pack.DataPack;

/**
 * A rough set filter operator.
 * 
 * User must call {@link #materialize(List)} before calling {@link #roughCheckOnColumn(InfoSegment)},
 * {@link #roughCheckOnPack(Segment)}, {@link #roughCheckOnPack(Segment, int)} and {@link #exactCheckOnRow(DataPack[])}.
 * e.g.
 * <pre>
 *     rsFilter.optimize();
 *     rsFilter.materialize(columnSchemas);
 *     byte res = rsFilter.roughCheckOnPack(segment, packId);
 * </pre>
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
        @JsonSubTypes.Type(name = "unknown", value = UnknownOperator.class),
        @JsonSubTypes.Type(name = "and", value = And.class),
        @JsonSubTypes.Type(name = "or", value = Or.class),
        @JsonSubTypes.Type(name = "not", value = Not.class),
        @JsonSubTypes.Type(name = "between", value = Between.class),
        @JsonSubTypes.Type(name = "not_between", value = NotBetween.class),
        @JsonSubTypes.Type(name = "equal", value = Equal.class),
        @JsonSubTypes.Type(name = "not_equal", value = NotEqual.class),
        @JsonSubTypes.Type(name = "greater", value = Greater.class),
        @JsonSubTypes.Type(name = "greater_equal", value = GreaterEqual.class),
        @JsonSubTypes.Type(name = "in", value = In.class),
        @JsonSubTypes.Type(name = "not_in", value = NotIn.class),
        @JsonSubTypes.Type(name = "less", value = Less.class),
        @JsonSubTypes.Type(name = "less_equal", value = LessEqual.class),
        @JsonSubTypes.Type(name = "like", value = Like.class),
        @JsonSubTypes.Type(name = "not_like", value = NotLike.class),
})
public interface RCOperator {

    @JsonProperty("type")
    String getType();

    /**
     * The {@link Attr}s of this op. Return empty if no attr.
     */
    Collection<Attr> attr();

    default List<RCOperator> children() {
        return Collections.emptyList();
    }

    // This method is here to make some json deserializer happy.
    @JsonSetter("type")
    default void setType(String type) {
        assert type.equals(getType());
    }

    default byte[] roughCheckOnPack(Segment segment) throws IOException {
        int packCount = segment.packCount();
        byte[] rsValues = new byte[packCount];
        for (int packId = 0; packId < packCount; packId++) {
            rsValues[packId] = roughCheckOnPack(segment, packId);
        }
        return rsValues;
    }

    byte roughCheckOnColumn(InfoSegment segment) throws IOException;

    byte roughCheckOnPack(Segment segment, int packId) throws IOException;

    byte roughCheckOnRow(DataPack[] rowPacks);

    BitSet exactCheckOnRow(DataPack[] rowPacks);

    /*
     * Apply not to this node.
     * 
     * e.g. "not (a >= b)" -> "a < b"
     */
    RCOperator applyNot();

    /*
     * Switch operator direction between operands.
     * 
     * e.g. "a >= b" -> "a <= b"
     */
    default RCOperator switchDirection() {return this;}

    /*
     * Optimize current node.
     * 
     * e.g. "a = 1 or a = 2 or a = 3" -> "a in (1, 2, 3)"
     */
    default RCOperator doOptimize() {return this;}

    /**
     * Optimize the whole tree. Call this method on root node after constructed a rc operator tree.
     * 
     * Generally child classes should not override this method.
     */
    default RCOperator optimize() {
        // Push not into operator
        return doOptimize().applyNot().applyNot().doOptimize();
    }

    /**
     * Make all attrs in this op and its children point to the real columns.
     */
    default void materialize(List<ColumnSchema> schemas) {
        for (Attr attr : attr()) {
            attr.materialize(schemas);
        }
        for (RCOperator op : children()) {
            op.materialize(schemas);
        }
    }

    /**
     * Runs the given function on this node and then recursively on {@link #children()}.
     */
    default void foreachEX(OpConsumer f) throws IOException {
        f.accept(this);
        for (RCOperator child : children()) {
            child.foreachEX(f);
        }
    }

    default void foreach(Consumer<RCOperator> f) {
        f.accept(this);
        for (RCOperator child : children()) {
            child.foreach(f);
        }
    }

    public static interface OpConsumer {
        void accept(RCOperator op) throws IOException;
    }
}
