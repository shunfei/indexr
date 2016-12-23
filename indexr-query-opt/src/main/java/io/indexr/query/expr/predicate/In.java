package io.indexr.query.expr.predicate;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.ArrayList;
import java.util.List;

import io.indexr.query.expr.Expression;
import io.indexr.query.row.InternalRow;
import io.indexr.query.types.DataType;

public class In extends Predicate {
    public Expression value;
    public List<Expression> list;

    public In(Expression value, List<Expression> list) {
        assert list != null;
        assert list.stream().map(e -> e.dataType() == value.dataType()).reduce(Boolean::logicalAnd).get();
        Preconditions.checkArgument(!list.isEmpty());
        this.value = value;
        this.list = list;
    }

    @Override
    public boolean evalBoolean(InternalRow input) {
        if (value.dataType() == DataType.StringType) {
            UTF8String thisVal = value.evalString(input);
            for (Expression expr : list) {
                UTF8String exprVal = expr.evalString(input);
                if (thisVal.equals(exprVal)) {
                    return true;
                }
            }
        } else {
            long thisVal = value.evalUniformVal(input);
            for (Expression expr : list) {
                long exprVal = expr.evalUniformVal(input);
                if (thisVal == exprVal) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public List<Expression> children() {
        ArrayList<Expression> children = new ArrayList<>(1 + list.size());
        children.add(value);
        children.addAll(list);
        return children;
    }

    @Override
    public Expression withNewChildren(List<Expression> newChildren) {
        assert newChildren.size() >= 2;
        return new In(newChildren.get(0), newChildren.subList(1, newChildren.size()));
    }

    @Override
    public List<Object> args() {return Lists.newArrayList(value, list);}

    @Override
    public String toString() {
        return String.format("%s IN (%s)", value, StringUtils.join(list, ','));
    }
}
