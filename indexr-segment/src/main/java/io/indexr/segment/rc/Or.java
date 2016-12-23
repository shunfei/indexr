package io.indexr.segment.rc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.indexr.segment.InfoSegment;
import io.indexr.segment.RSValue;
import io.indexr.segment.Segment;
import io.indexr.segment.pack.DataPack;

public class Or implements LogicalOperator {
    @JsonProperty("children")
    public final List<RCOperator> children;

    @JsonCreator
    public Or(@JsonProperty("children") List<RCOperator> children) {
        this.children = children;
    }

    @Override
    public String getType() {
        return "or";
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
        return new And(newOps);
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
        Or newOr = hasUpdate ? new Or(newOps) : this;

        RCOperator res = null;
        if ((res = newOr.tryToIn()) != null) {
            return res;
        } else if ((res = newOr.tryToNotBetween()) != null) {
            return res;
        } else if ((res = newOr.tryToGreaterEqual()) != null) {
            return res;
        } else if ((res = newOr.tryToLessEqual()) != null) {
            return res;
        } else {
            return newOr;
        }
    }

    private RCOperator tryToIn() {
        // "a = 1 or a = 2 or a = 3 or a < -100" -> "(a in (1, 2, 3)) or (a < -100)"

        if (children.size() <= 2) {
            return null;
        }

        Map<Attr, List<Equal>> equalLists = new HashMap<>();
        List<RCOperator> newChildren = new ArrayList<>();
        for (RCOperator op : children) {
            if (op.getClass() == Equal.class) {
                Equal equal = (Equal) op;
                List<Equal> list = equalLists.get(equal.attr);
                if (list == null) {
                    list = new ArrayList<>();
                    equalLists.put(equal.attr, list);
                }
                list.add(equal);
            } else {
                newChildren.add(op);
            }
        }
        for (Map.Entry<Attr, List<Equal>> e : equalLists.entrySet()) {
            Attr attr = e.getKey();
            List<Equal> list = e.getValue();
            if (list.size() == 1) {
                newChildren.add(list.get(0));
                continue;
            }
            long[] numValues = new long[list.size()];
            UTF8String[] strValues = new UTF8String[list.size()];
            for (int i = 0; i < numValues.length; i++) {
                numValues[i] = list.get(i).numValue;
                strValues[i] = list.get(i).strValue;
            }
            newChildren.add(new In(attr, numValues, strValues));
        }

        if (newChildren.size() == 1) {
            return newChildren.get(0);
        } else {
            return new Or(newChildren);
        }
    }

    private RCOperator tryToNotBetween() {
        // "a < 10 or a > 20" -> "not(a>=10 and a <= 20)" -> "a not_between(10, 20)"
        if (children.size() != 2) {
            return null;
        }
        RCOperator left = children.get(0);
        RCOperator right = children.get(1);
        if (left.getClass() == Less.class && right.getClass() == Greater.class) {
            Less l = (Less) left;
            Greater g = (Greater) right;
            if (l.attr.equals(g.attr)) {
                return new NotBetween(l.attr, l.numValue, g.numValue, l.strValue, g.strValue);
            }
        } else if (left.getClass() == Greater.class && right.getClass() == Less.class) {
            Greater g = (Greater) left;
            Less l = (Less) right;
            if (l.attr.equals(g.attr)) {
                return new NotBetween(l.attr, l.numValue, g.numValue, l.strValue, g.strValue);
            }
        }
        return null;
    }

    private RCOperator tryToGreaterEqual() {
        // "a == 0 or a > 0" -> "a >= 0"
        if (children.size() != 2) {
            return null;
        }
        RCOperator left = children.get(0);
        RCOperator right = children.get(1);
        if (left.getClass() == Equal.class && right.getClass() == Greater.class) {
            Equal e = (Equal) left;
            Greater g = (Greater) right;
            if (e.attr.equals(g.attr)
                    && (e.numValue == g.numValue
                    && e.strValue == null ? g.strValue == null : e.strValue.equals(g.strValue))) {
                return new GreaterEqual(e.attr, e.numValue, e.strValue);
            }
        } else if (left.getClass() == Greater.class && right.getClass() == Equal.class) {
            Equal e = (Equal) right;
            Greater g = (Greater) left;
            if (e.attr.equals(g.attr)
                    && (e.numValue == g.numValue
                    && e.strValue == null ? g.strValue == null : e.strValue.equals(g.strValue))) {
                return new GreaterEqual(e.attr, e.numValue, e.strValue);
            }
        }
        return null;
    }

    private RCOperator tryToLessEqual() {
        // "a == 0 or a < 0" -> "a <= 0"
        if (children.size() != 2) {
            return null;
        }
        RCOperator left = children.get(0);
        RCOperator right = children.get(1);
        if (left.getClass() == Equal.class && right.getClass() == Less.class) {
            Equal e = (Equal) left;
            Less l = (Less) right;
            if (e.attr.equals(l.attr)
                    && (e.numValue == l.numValue
                    && e.strValue == null ? l.strValue == null : e.strValue.equals(l.strValue))) {
                return new LessEqual(e.attr, e.numValue, e.strValue);
            }
        } else if (left.getClass() == Less.class && right.getClass() == Equal.class) {
            Equal e = (Equal) right;
            Less l = (Less) left;
            if (e.attr.equals(l.attr)
                    && (e.numValue == l.numValue
                    && e.strValue == null ? l.strValue == null : e.strValue.equals(l.strValue))) {
                return new GreaterEqual(e.attr, e.numValue, e.strValue);
            }
        }
        return null;
    }

    @Override
    public byte roughCheckOnPack(Segment segment, int packId) throws IOException {
        boolean hasSome = false;
        for (RCOperator op : children) {
            byte v = op.roughCheckOnPack(segment, packId);
            if (v == RSValue.All) {
                return RSValue.All;
            } else if (v == RSValue.Some) {
                hasSome = true;
            }
        }
        return hasSome ? RSValue.Some : RSValue.None;
    }

    @Override
    public byte roughCheckOnColumn(InfoSegment segment) throws IOException {
        boolean hasSome = false;
        for (RCOperator op : children) {
            byte v = op.roughCheckOnColumn(segment);
            if (v == RSValue.All) {
                return RSValue.All;
            } else if (v == RSValue.Some) {
                hasSome = true;
            }
        }
        return hasSome ? RSValue.Some : RSValue.None;
    }

    @Override
    public byte roughCheckOnRow(DataPack[] rowPacks) {
        boolean hasSome = false;
        for (RCOperator op : children) {
            byte v = op.roughCheckOnRow(rowPacks);
            if (v == RSValue.All) {
                return RSValue.All;
            } else if (v == RSValue.Some) {
                hasSome = true;
            }
        }
        return hasSome ? RSValue.Some : RSValue.None;
    }

    @Override
    public BitSet exactCheckOnRow(DataPack[] rowPacks) {
        BitSet res = null;
        for (RCOperator op : children) {
            BitSet bitSet = op.exactCheckOnRow(rowPacks);
            if (res == null) {
                res = bitSet;
            } else {
                res.or(bitSet);
            }
        }
        return res;
    }

    @Override
    public String toString() {
        return String.format("Or[%s]", children.stream().map(Object::toString).collect(Collectors.joining(",")));
    }
}
