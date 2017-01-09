package io.indexr.query.plan;

import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.indexr.query.TreeNode;
import io.indexr.query.expr.Expression;
import io.indexr.query.expr.attr.Attribute;
import io.indexr.util.function.PartialFunction;

import static io.indexr.util.Trick.flatMapToList;
import static io.indexr.util.Trick.mapToList;

public abstract class QueryPlan<PlanType extends QueryPlan<PlanType>> extends TreeNode<PlanType> {
    public abstract List<Attribute> output();

    /** Returns the set of attributes that are output by this node. */
    public Set<Attribute> outputSet() {
        return new HashSet<>(output());
    }

    /**
     * All Attributes that appear in expressions from this operator.  Note that this set does not
     * include attributes that are implicitly referenced by being passed through to the output tuple.
     */
    public Set<Attribute> references() {
        return expressions().stream().flatMap(e -> e.references().stream()).collect(Collectors.toSet());
    }

    /** The set of all attributes that are input to this operator by its children. */
    public Set<Attribute> inputSet() {
        return children().stream().flatMap(e -> e.output().stream()).collect(Collectors.toSet());
    }

    /** The set of all attributes that are produced by this node. */
    public Set<Attribute> producedAttributes() {
        return Collections.emptySet();
    }

    /**
     * Attributes that are referenced by expressions but not provided by this nodes children.
     * Subclasses should override this method if they produce attributes internally as it is used by
     * assertions designed to prevent the construction of invalid plans.
     */
    public Set<Attribute> missingInput() {
        Set<Attribute> attrs = references();
        attrs.removeAll(inputSet());
        attrs.removeAll(producedAttributes());
        return attrs;
    }

    public StructType schema() {
        return StructType.fromAttributes(output());
    }

    private static List<Expression> collectionToExprs(Collection<?> c) {
        return flatMapToList(c, e -> {
            if (e == null) {
                return Collections.emptyList();
            }
            if (e instanceof Expression) {
                return Collections.singletonList((Expression) e);
            }
            if (e instanceof Collection) {
                return collectionToExprs((Collection) e);
            }
            return Collections.emptyList();
        }, /*ignoreNull*/ true);
    }

    /** Returns all of the expressions present in this query plan operator. */
    public List<Expression> expressions() {
        return flatMapToList(args(), a -> {
            if (a == null) {
                return Collections.emptyList();
            }
            if (a instanceof Expression) {
                return Collections.singletonList((Expression) a);
            }
            if (a instanceof Collection) {
                return collectionToExprs((Collection) a);
            }
            return Collections.emptyList();
        }, /*ignoreNull*/ true);
    }

    /**
     * Runs [[transform]] with `rule` on all expressions present in this query operator.
     * Users should not expect a specific directionality. If a specific directionality is needed,
     * transformExpressionsDown or transformExpressionsUp should be used.
     *
     * @param rule the rule to be applied to every expression in this operator.
     */
    public PlanType transformExpressions(PartialFunction<Expression, Expression> rule) {
        return transformExpressionsDown(rule);
    }

    public PlanType transformExpressions(Function<Expression, Expression> rule) {
        return transformExpressions(PartialFunction.fromFunction(rule));
    }

    private static class ExpressionTransformeDown implements Function {
        PartialFunction<Expression, Expression> rule;
        boolean changed = false;

        ExpressionTransformeDown(PartialFunction<Expression, Expression> rule) {
            this.rule = rule;
        }

        Expression transform(Expression e) {
            Expression newE = e.transformDown(rule);
            if (e.fastEquals(newE)) {
                return e;
            } else {
                changed = true;
                return newE;
            }
        }

        @Override
        public Object apply(Object arg) {
            if (arg == null) {
                return null;
            } else if (arg instanceof Expression) {
                return transform((Expression) arg);
            } else if (arg instanceof List) {
                List res = new ArrayList<>();
                for (Object o : (List) arg) {
                    res.add(this.apply(o));
                }
                return res;
            } else {
                return arg;
            }
        }
    }


    /**
     * Runs [[transformDown]] with `rule` on all expressions present in this query operator.
     *
     * @param rule the rule to be applied to every expression in this operator.
     */
    public PlanType transformExpressionsDown(PartialFunction<Expression, Expression> rule) {
        ExpressionTransformeDown transform = new ExpressionTransformeDown(rule);
        List<Object> newArgs = mapToList(args(), transform);
        return transform.changed ? withNewArgs(newArgs) : (PlanType) this;
    }

    public PlanType transformExpressionsDown(Function<Expression, Expression> rule) {
        return transformExpressionsDown(PartialFunction.fromFunction(rule));
    }

    private static class ExpressionTransformeUp implements Function {
        PartialFunction<Expression, Expression> rule;
        boolean changed = false;

        ExpressionTransformeUp(PartialFunction<Expression, Expression> rule) {
            this.rule = rule;
        }

        Expression transform(Expression e) {
            Expression newE = e.transformDown(rule);
            if (e.fastEquals(newE)) {
                return e;
            } else {
                changed = true;
                return newE;
            }
        }

        @Override
        public Object apply(Object arg) {
            if (arg == null) {
                return null;
            } else if (arg instanceof Expression) {
                return transform((Expression) arg);
            } else if (arg instanceof List) {
                List res = new ArrayList<>();
                for (Object o : (List) arg) {
                    res.add(this.apply(o));
                }
                return res;
            } else {
                return arg;
            }
        }
    }

    /**
     * Runs [[transformUp]] with `rule` on all expressions present in this query operator.
     *
     * @param rule the rule to be applied to every expression in this operator.
     */
    public PlanType transformExpressionsUp(PartialFunction<Expression, Expression> rule) {
        ExpressionTransformeUp transform = new ExpressionTransformeUp(rule);
        List<Object> newArgs = mapToList(args(), transform);
        return transform.changed ? withNewArgs(newArgs) : (PlanType) this;
    }

    public PlanType transformExpressionsUp(Function<Expression, Expression> rule) {
        return transformExpressionsUp(PartialFunction.fromFunction(rule));
    }

    /**
     * Returns the result of running [[transformExpressions]] on this node
     * and all its children.
     */
    public PlanType transformAllExpressions(PartialFunction<Expression, Expression> rule) {
        return transform(new PartialFunction<PlanType, PlanType>() {
            @Override
            public boolean isDefinedAt(PlanType x) {
                return x instanceof QueryPlan;
            }

            @Override
            public PlanType apply(PlanType plan) {
                return plan.transformExpressions(rule);
            }
        });
    }

    public PlanType transformAllExpressions(Function<Expression, Expression> rule) {
        return transformAllExpressions(PartialFunction.fromFunction(rule));
    }

    /**
     * A prefix string used when printing the plan.
     * 
     * We use "!" to indicate an invalid plan, and "'" to indicate an unresolved plan.
     */
    protected String statePrefix() {
        return !missingInput().isEmpty() && !children().isEmpty() ? "!" : "";
    }

    @Override
    public String simpleString() {
        return statePrefix() + super.simpleString();
    }

}
