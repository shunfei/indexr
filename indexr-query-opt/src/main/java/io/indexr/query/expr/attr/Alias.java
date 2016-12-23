package io.indexr.query.expr.attr;

import com.google.common.collect.Lists;

import org.apache.spark.sql.types.Metadata;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.Collections;
import java.util.List;

import io.indexr.query.expr.Expression;
import io.indexr.query.row.InternalRow;
import io.indexr.query.types.DataType;

public class Alias extends NamedExpression {
    public Expression child;
    public String name;
    public long exprId;

    public Alias(Expression child, String name) {
        this(child, name, NamedExpression.newExprId());
    }

    public Alias(Expression child, String name, long exprId) {
        this.child = child;
        this.name = name;
        this.exprId = exprId;
    }

    @Override
    public String name() {return name;}

    @Override
    public long exprId() {return exprId;}

    @Override
    public DataType dataType() {return child.dataType();}

    @Override
    public List<Expression> children() {return Collections.singletonList(child);}

    @Override
    public Expression withNewChildren(List<Expression> newChildren) {
        assert newChildren.size() == 1;
        return new Alias(newChildren.get(0), name, exprId);
    }

    @Override
    public List<Object> args() {return Lists.newArrayList(child, name, exprId);}

    @Override
    public Attribute toAttribute() {
        return resolved()
                ? new AttributeReference(name, child.dataType(), Metadata.empty, exprId)
                : new UnresolvedAttribute(name);
    }

    @Override
    public NamedExpression newInstance() {
        return new Alias(child, name);
    }

    @Override
    public String toString() {
        return String.format("alias(%s->%s#%s)", child, name, exprId);
    }


    // @formatter:off
    @Override public long evalUniformVal(InternalRow input) {return child.evalUniformVal(input);}
    @Override public Object eval(InternalRow input) {return child.eval(input);}
    @Override public boolean evalBoolean(InternalRow input) {return child.evalBoolean(input);}
    @Override public int evalInt(InternalRow input) {return child.evalInt(input);}
    @Override public long evalLong(InternalRow input) {return child.evalLong(input);}
    @Override public float evalFloat(InternalRow input) {return child.evalFloat(input);}
    @Override public double evalDouble(InternalRow input) {return child.evalDouble(input);}
    @Override public UTF8String evalString(InternalRow input) {return child.evalString(input);}
    // @formatter:on
}
