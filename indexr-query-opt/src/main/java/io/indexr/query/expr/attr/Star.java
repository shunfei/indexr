package io.indexr.query.expr.attr;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import io.indexr.query.Resolver;
import io.indexr.query.UnresolvedException;
import io.indexr.query.expr.Expression;
import io.indexr.query.plan.logical.LogicalPlan;
import io.indexr.query.row.InternalRow;
import io.indexr.query.types.DataType;

public class Star extends NamedExpression {
    // @formatter:off
    @Override public long exprId() {throw new UnresolvedException(this, "exprId");}
    @Override public String name() {throw new UnresolvedException(this, "name");}
    @Override public DataType dataType() {throw new UnresolvedException(this, "dataType");}
    @Override public long evalUniformVal(InternalRow input) {throw new UnresolvedException(this, "eval");}
    @Override public Attribute toAttribute() {throw new UnresolvedException(this, "toAttribute");}
    @Override public NamedExpression newInstance() {throw new UnresolvedException(this, "newInstance");}
    @Override public boolean resolved() {return false;}
    @Override public List<Expression> children() {return Collections.emptyList();}
    @Override public Expression withNewChildren(List<Expression> newChildren) {return new Star();}
    @Override public List<Object> args() {return Collections.emptyList();}
    // @formatter:on

    public List<NamedExpression> expand(LogicalPlan input, Resolver resolver) {
        return new ArrayList<>(input.output());
    }

    @Override
    public String toString() {
        return "*";
    }
}
