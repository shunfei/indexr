package io.indexr.segment.rc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.stream.Collectors;

import io.indexr.segment.InfoSegment;
import io.indexr.segment.RSValue;
import io.indexr.segment.Segment;

public class And implements LogicalOperator {
    @JsonProperty("children")
    public final List<RCOperator> children;

    @JsonCreator
    public And(@JsonProperty("children") List<RCOperator> children) {
        this.children = children;
    }

    @Override
    public String getType() {
        return "and";
    }

    @Override
    public List<RCOperator> children() {
        return children;
    }

    @Override
    public RCOperator applyNot() {
        List<RCOperator> newOps = new ArrayList<>();
        for (RCOperator op : children) {
            newOps.add(op.applyNot());
        }
        return new Or(newOps);
    }

    @Override
    public RCOperator doOptimize() {
        List<RCOperator> newOps = new ArrayList<>();
        boolean hasUpdate = false;
        for (RCOperator op : children) {
            RCOperator newOp = op.doOptimize();
            newOps.add(newOp);
            if (newOp != op) {
                hasUpdate = true;
            }
        }
        And newAnd = hasUpdate ? new And(newOps) : this;

        RCOperator res = null;
        if ((res = newAnd.tryToNotIn()) != null) {
            return res;
        } else if ((res = newAnd.tryToBetween()) != null) {
            return res;
        } else {
            return newAnd;
        }
    }

    private RCOperator tryToNotIn() {
        // "a != 1 and a != 2 and a != 3" -> "a not in (1, 2, 3)"
        boolean ok = true;
        Attr attr = null;
        long[] numValues = new long[children.size()];
        UTF8String[] strValues = new UTF8String[children.size()];
        int i = 0;
        for (RCOperator op : children) {
            if (op.getClass() == NotEqual.class) {
                NotEqual equal = (NotEqual) op;
                if (attr == null) {
                    attr = equal.attr;
                }
                if (!equal.attr.equals(attr)) {
                    ok = false;
                    break;
                }
                numValues[i] = equal.numValue;
                strValues[i] = equal.strValue;
            } else {
                ok = false;
                break;
            }
            i++;
        }
        if (!ok) {
            return null;
        }
        return new NotIn(attr, numValues, strValues);
    }

    private RCOperator tryToBetween() {
        // "a >= 10 and a <= 20" -> "a between(10, 20)"
        if (children.size() != 2) {
            return null;
        }
        RCOperator left = children.get(0);
        RCOperator right = children.get(1);
        if (left.getClass() == GreaterEqual.class && right.getClass() == LessEqual.class) {
            GreaterEqual ge = (GreaterEqual) left;
            LessEqual le = (LessEqual) right;
            if (ge.attr.equals(le.attr)) {
                return new Between(ge.attr, ge.numValue, le.numValue, ge.strValue, le.strValue);
            }
        } else if (left.getClass() == LessEqual.class && right.getClass() == GreaterEqual.class) {
            GreaterEqual ge = (GreaterEqual) right;
            LessEqual le = (LessEqual) left;
            if (ge.attr.equals(le.attr)) {
                return new Between(ge.attr, ge.numValue, le.numValue, ge.strValue, le.strValue);
            }
        }
        return null;
    }

    @Override
    public byte roughCheckOnPack(Segment segment, int packId) throws IOException {
        boolean hasSome = false;
        for (RCOperator op : children) {
            byte v = op.roughCheckOnPack(segment, packId);
            if (v == RSValue.None) {
                return RSValue.None;
            } else if (v == RSValue.Some) {
                hasSome = true;
            }
        }
        return hasSome ? RSValue.Some : RSValue.All;
    }

    @Override
    public byte roughCheckOnColumn(InfoSegment segment) throws IOException {
        boolean hasSome = false;
        for (RCOperator op : children) {
            byte v = op.roughCheckOnColumn(segment);
            if (v == RSValue.None) {
                return RSValue.None;
            } else if (v == RSValue.Some) {
                hasSome = true;
            }
        }
        return hasSome ? RSValue.Some : RSValue.All;
    }

    @Override
    public byte roughCheckOnRow(Segment segment, int packId) throws IOException {
        boolean hasSome = false;
        for (RCOperator op : children) {
            byte v = op.roughCheckOnRow(segment, packId);
            if (v == RSValue.None) {
                return RSValue.None;
            } else if (v == RSValue.Some) {
                hasSome = true;
            }
        }
        return hasSome ? RSValue.Some : RSValue.All;
    }

    @Override
    public BitSet exactCheckOnRow(Segment segment, int packId) throws IOException {
        BitSet res = null;
        for (RCOperator op : children) {
            BitSet bitSet = op.exactCheckOnRow(segment, packId);
            if (res == null) {
                res = bitSet;
            } else {
                res.and(bitSet);
            }
        }
        return res;
    }

    @Override
    public String toString() {
        return String.format("And[%s]", children.stream().map(Object::toString).collect(Collectors.joining(",")));
    }
}
