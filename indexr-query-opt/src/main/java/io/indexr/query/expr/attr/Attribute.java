package io.indexr.query.expr.attr;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import io.indexr.query.expr.Expression;
import io.indexr.query.row.InternalRow;

public abstract class Attribute extends NamedExpression {

    @Override
    public final List<Expression> children() {return Collections.emptyList();}

    @Override
    public abstract Attribute withNewChildren(List<Expression> newChildren);

    @Override
    public final Set<Attribute> references() {return Collections.singleton(this);}

    @Override
    public final long evalUniformVal(InternalRow input) {
        throw new UnsupportedOperationException(String.format("Cannot evaluate expression: %s", this));
    }

    @Override
    public Attribute toAttribute() {return this;}

    @Override
    public abstract Attribute newInstance();

    public abstract Attribute withName(String newName);
}
