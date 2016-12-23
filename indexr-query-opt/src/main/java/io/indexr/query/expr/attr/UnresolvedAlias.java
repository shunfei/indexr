package io.indexr.query.expr.attr;

import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;

import javax.annotation.Nullable;

import io.indexr.query.UnresolvedException;
import io.indexr.query.expr.Expression;
import io.indexr.query.row.InternalRow;
import io.indexr.query.types.DataType;

public class UnresolvedAlias extends NamedExpression {
    public Expression child;
    @Nullable
    public String aliasName;

    public UnresolvedAlias(Expression child, String aliasName) {
        this.child = child;
        this.aliasName = aliasName;
    }

    @Override
    public boolean resolved() {return false;}

    @Override
    public List<Expression> children() {return Collections.singletonList(child);}

    @Override
    public Expression withNewChildren(List<Expression> newChildren) {
        assert newChildren.size() == 1;
        return new UnresolvedAlias(newChildren.get(0), aliasName);
    }

    @Override
    public List<Object> args() {return Lists.newArrayList(child, aliasName);}

    // @formatter:off
    @Override public long evalUniformVal(InternalRow input) {throw new UnresolvedException(this, "evalUniformVal");}
    @Override public String name() {throw new UnresolvedException(this, "name");}
    @Override public long exprId() {throw new UnresolvedException(this, "exprId");}
    @Override public Attribute toAttribute() {throw new UnresolvedException(this, "toAttribute");}
    @Override public DataType dataType() {throw new UnresolvedException(this, "dataType");}
    @Override public NamedExpression newInstance() {throw new UnresolvedException(this, "newInstance");}
    // @formatter:on
}
