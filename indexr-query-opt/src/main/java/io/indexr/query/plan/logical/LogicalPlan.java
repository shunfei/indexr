package io.indexr.query.plan.logical;

import java.util.List;
import java.util.function.Function;

import io.indexr.query.Resolver;
import io.indexr.query.expr.Expression;
import io.indexr.query.expr.attr.Attribute;
import io.indexr.query.expr.attr.NamedExpression;
import io.indexr.query.plan.QueryPlan;
import io.indexr.util.function.PartialFunction;

import static io.indexr.util.Trick.flatMapToList;
import static io.indexr.util.Trick.forAll;
import static io.indexr.util.Trick.identity;

public abstract class LogicalPlan extends QueryPlan<LogicalPlan> {

    private boolean _analyzed = false;

    /**
     * Marks this plan as already analyzed.  This should only be called by CheckAnalysis.
     */
    public void setAnalyzed() { _analyzed = true;}

    /**
     * Returns true if this node and its children have already been gone through analysis and
     * verification.  Note that this is only an optimization used to avoid analyzing trees that
     * have already been analyzed, and can be reset by transformations.
     */
    public boolean analyzed() {return _analyzed;}

    /**
     * Returns a copy of this node where `rule` has been recursively applied first to all of its
     * children and then itself (post-order). When `rule` does not apply to a given node, it is left
     * unchanged.  This function is similar to `transformUp`, but skips sub-trees that have already
     * been marked as analyzed.
     *
     * @param rule the function use to transform this nodes children
     */
    public LogicalPlan resolveOperators(PartialFunction<LogicalPlan, LogicalPlan> rule) {
        if (!_analyzed) {
            LogicalPlan afterRuleOnChildren = transformChildren(rule, LogicalPlan::resolveOperators);
            if (this.fastEquals(afterRuleOnChildren)) {
                return rule.applyOrElse(this, identity());
            } else {
                return rule.applyOrElse(afterRuleOnChildren, identity());
            }
        } else {
            return this;
        }
    }

    public LogicalPlan resolveOperators(Function<LogicalPlan, LogicalPlan> rule) {
        return resolveOperators(PartialFunction.fromFunction(rule));
    }

    /**
     * Recursively transforms the expressions of a tree, skipping nodes that have already
     * been analyzed.
     */
    public LogicalPlan resolveExpressions(PartialFunction<Expression, Expression> rule) {
        return this.resolveOperators(p -> p.transformExpressions(rule));
    }

    public LogicalPlan resolveExpressions(Function<Expression, Expression> rule) {
        return resolveExpressions(PartialFunction.fromFunction(rule));
    }

    @Override
    public String nodeName() {
        return "logical." + getClass().getSimpleName();
    }

    /**
     * Returns true if this expression and all its children have been resolved to a specific schema
     * and false if it still contains any unresolved placeholders. Implementations of LogicalPlan
     * can override this (e.g.
     * [[org.apache.spark.sql.catalyst.analysis.UnresolvedRelation UnresolvedRelation]]
     * should return `false`).
     */
    public boolean resolved() {
        return forAll(expressions(), Expression::resolved) && childrenResolved();
    }

    @Override
    protected String statePrefix() {
        return !resolved() ? "'" : super.statePrefix();
    }

    /**
     * Returns true if all its children of this query plan have been resolved.
     */
    public boolean childrenResolved() {
        return forAll(children(), LogicalPlan::resolved);
    }


    public NamedExpression resolveChildren(
            String nameParts,
            Resolver resolver) {
        return resolve(nameParts, flatMapToList(children(), QueryPlan::output), resolver);
    }

    public NamedExpression resolve(
            String nameParts,
            Resolver resolver) {
        return resolve(nameParts, output(), resolver);
    }

    protected NamedExpression resolve(
            String name,
            List<Attribute> input,
            Resolver resolver) {
        for (Attribute attr : input) {
            if (resolver.resolve(name, attr.name())) {
                return attr.withName(name);
            }
        }
        return null;
    }
}