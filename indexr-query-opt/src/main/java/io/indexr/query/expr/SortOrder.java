package io.indexr.query.expr;

import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;

import io.indexr.query.row.InternalRow;
import io.indexr.query.types.DataType;

public class SortOrder extends Expression {
    public Expression child;
    public SortDirection direction;

    public SortOrder(Expression child, SortDirection direction) {
        this.child = child;
        this.direction = direction;
    }

    @Override
    public List<Expression> children() {
        return Collections.singletonList(child);
    }

    @Override
    public Expression withNewChildren(List<Expression> newChildren) {
        assert newChildren.size() == 1;
        return new SortOrder(newChildren.get(0), direction);
    }

    @Override
    public List<Object> args() {
        return Lists.newArrayList(child, direction);
    }

    @Override
    public DataType dataType() {
        return child.dataType();
    }

    public boolean isAscending() {
        return direction == SortDirection.Ascending;
    }

    @Override
    public long evalUniformVal(InternalRow input) {
        throw new UnsupportedOperationException(String.format("Cannot evaluate expression: %s", this));
    }

    public static enum SortDirection {
        Ascending("ASC"),
        Descending("DESC");

        public final String sql;

        SortDirection(String sql) {
            this.sql = sql;
        }
    }
}
