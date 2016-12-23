package io.indexr.query.expr.attr;

import java.util.concurrent.atomic.AtomicLong;

import io.indexr.query.expr.Expression;

public abstract class NamedExpression extends Expression {

    public abstract String name();

    public abstract long exprId();

    public abstract Attribute toAttribute();

    /** Returns a copy of this expression with a new `exprId`. */
    public abstract NamedExpression newInstance();

    private static AtomicLong curExprId = new AtomicLong();

    public static long newExprId() {return curExprId.incrementAndGet();}
}
