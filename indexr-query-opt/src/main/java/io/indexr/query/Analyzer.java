package io.indexr.query;

import com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.indexr.query.expr.Cast;
import io.indexr.query.expr.Expression;
import io.indexr.query.expr.SortOrder;
import io.indexr.query.expr.agg.AggregateExpression;
import io.indexr.query.expr.attr.Alias;
import io.indexr.query.expr.attr.Attribute;
import io.indexr.query.expr.attr.NamedExpression;
import io.indexr.query.expr.attr.Star;
import io.indexr.query.expr.attr.UnresolvedAlias;
import io.indexr.query.expr.attr.UnresolvedAttribute;
import io.indexr.query.plan.logical.Aggregate;
import io.indexr.query.plan.logical.LogicalPlan;
import io.indexr.query.plan.logical.Project;
import io.indexr.query.plan.logical.Sort;
import io.indexr.query.plan.logical.UnresolvedRelation;
import io.indexr.util.Pair;

import static io.indexr.util.Trick.compareList;
import static io.indexr.util.Trick.concatToList;
import static io.indexr.util.Trick.empty;
import static io.indexr.util.Trick.filterToList;
import static io.indexr.util.Trick.filterToSet;
import static io.indexr.util.Trick.flatMapToList;
import static io.indexr.util.Trick.indexWhere;
import static io.indexr.util.Trick.mapToList;
import static io.indexr.util.Trick.mapToSet;
import static io.indexr.util.Trick.up;

public class Analyzer extends RuleExecutor<LogicalPlan> {
    private static final Logger log = LoggerFactory.getLogger(Analyzer.class);

    private Catalog catalog;

    public Analyzer(Catalog catalog) {
        this.catalog = catalog;
    }

    private Resolver resolver = Resolver.caseInsensitiveResolution;
    private Strategy fixedPoint = new Strategy(100);

    @Override
    protected List<Batch> batches() {
        return Lists.newArrayList(
                new Batch("Resolution", fixedPoint,
                        new ResolveRelations(),
                        new ResolveReferences(),
                        new ResolveSortReferences(),
                        new ResolveAliases(),
                        new GlobalAggregates(),
                        new ResolveAggregateFunctions()),
                new Batch("Cleanup", fixedPoint,
                        new CleanupAliases())
        );
    }

    /**
     * Replaces [[UnresolvedAlias]]s with concrete aliases.
     */
    private class ResolveAliases implements Rule<LogicalPlan> {
        private List<NamedExpression> assignAliases(List<NamedExpression> exprs) {
            ArrayList<NamedExpression> res = new ArrayList<>(exprs.size());
            int i = 0;
            for (NamedExpression expr : exprs) {
                final int exprOrdinal = i;
                res.add((NamedExpression) expr.transformUp(e -> {
                    if (!(e instanceof UnresolvedAlias)) {
                        return e;
                    }
                    Expression child = ((UnresolvedAlias) e).child;
                    String aliasName = ((UnresolvedAlias) e).aliasName;
                    if (child instanceof NamedExpression) {
                        return child;
                    }
                    if (!child.resolved()) {
                        return e;
                    }
                    if (child instanceof Cast && ((Cast) child).child instanceof NamedExpression) {
                        Cast ce = (Cast) child;
                        return new Alias(ce, ((NamedExpression) ce.child).name());
                    }
                    return new Alias(child, aliasName == null ? "_c" + exprOrdinal : aliasName);
                }));
                i++;
            }
            return res;
        }

        private boolean hasUnresolvedAlias(List<NamedExpression> exprs) {
            for (NamedExpression e : exprs) {
                if (e instanceof UnresolvedAlias) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public LogicalPlan apply(LogicalPlan plan) {
            return plan.resolveOperators(p -> {
                if (p instanceof Aggregate) {
                    Aggregate agg = (Aggregate) p;
                    if (agg.child.resolved() && hasUnresolvedAlias(agg.aggregateExpressions)) {
                        return new Aggregate(agg.groupingExpressions, assignAliases(agg.aggregateExpressions), agg.child);
                    }
                }
                if (p instanceof Project) {
                    Project project = (Project) p;
                    if (project.child.resolved() && hasUnresolvedAlias(project.projectList)) {
                        return new Project(assignAliases(project.projectList), project.child);
                    }
                }
                return p;
            });
        }
    }

    /**
     * Replaces [[UnresolvedRelation]]s with concrete relations from the catalog.
     */
    private class ResolveRelations implements Rule<LogicalPlan> {
        @Override
        public LogicalPlan apply(LogicalPlan plan) {
            return plan.resolveOperators(p -> {
                if (p instanceof UnresolvedRelation) {
                    return catalog.lookupRelation(((UnresolvedRelation) p).tableName());
                }
                return p;
            });
        }
    }

    /**
     * Replaces [[UnresolvedAttribute]]s with concrete [[AttributeReference]]s from
     * a logical plan node's children.
     */
    private class ResolveReferences implements Rule<LogicalPlan> {
        /**
         * Foreach expression, expands the matching attribute.*'s in `child`'s input for the subtree
         * rooted at each expression.
         */
        List<Expression> expandStarExpressions(List<? extends Expression> exprs, LogicalPlan child) {
            return flatMapToList(exprs, e -> {
                if (e instanceof Star) {
                    return ((Star) e).expand(child, resolver);
                }
                return Collections.singleton(e);
            });
        }

        List<NamedExpression> newAliases(List<NamedExpression> expressions) {
            return mapToList(expressions, e ->
                    e instanceof Alias
                            ? new Alias(((Alias) e).child, ((Alias) e).name())
                            : e);
        }

        Set<Attribute> findAliases(List<NamedExpression> projectList) {
            return mapToSet(projectList, e -> e instanceof Alias ? e.toAttribute() : null, true);
        }

        /**
         * Returns true if `exprs` contains a [[Star]].
         */
        boolean containsStar(List<? extends Expression> exprs) {
            return indexWhere(exprs, e -> e instanceof Star) >= 0;
        }

        @Override
        public LogicalPlan apply(LogicalPlan oldPlan) {
            return oldPlan.resolveOperators(plan -> {
                if (!plan.childrenResolved()) {
                    return plan;
                }
                // If the projection list contains Stars, expand it.
                if (plan instanceof Project) {
                    Project p = (Project) plan;
                    List<NamedExpression> projectList = p.projectList;
                    LogicalPlan child = p.child();
                    if (containsStar(projectList)) {
                        List<NamedExpression> newProjectList = flatMapToList(projectList, e -> {
                            if (e instanceof Star) {
                                return ((Star) e).expand(child, resolver);
                            }
                            return Collections.singletonList(e);
                        });
                        return new Project(newProjectList, p.child());
                    }
                }
                if (plan instanceof Aggregate) {
                    Aggregate agg = (Aggregate) plan;
                    List<NamedExpression> aggs = agg.aggregateExpressions;
                    if (containsStar(aggs)) {
                        return new Aggregate(
                                agg.groupingExpressions,
                                mapToList(expandStarExpressions(aggs, agg.child()), e -> (NamedExpression) e),
                                agg.child());
                    }
                }
                if (plan instanceof Sort) {
                    Sort sort = (Sort) plan;
                    if (sort.child().resolved() && !sort.resolved()) {
                        List<SortOrder> newOrdering = resolveSortOrders(sort.order, sort.child());
                        return new Sort(newOrdering, sort.child());
                    }
                }
                return plan.transformExpressionsUp(p -> {
                    if (p instanceof UnresolvedAttribute) {
                        NamedExpression result = plan.resolveChildren(((UnresolvedAttribute) p).name(), resolver);
                        log.trace("Resolving {} to {}", p, result);
                        if (result != null) {
                            return result;
                        }
                    }
                    return p;
                });
            });
        }
    }

    private List<SortOrder> resolveSortOrders(List<SortOrder> ordering, LogicalPlan plan) {
        return mapToList(ordering, order -> {
            return (SortOrder) order.transformUp(e -> {
                if (e instanceof UnresolvedAttribute) {
                    NamedExpression resolved = plan.resolve(((UnresolvedAttribute) e).name(), resolver);
                    if (resolved != null) {
                        return resolved;
                    }
                }
                return e;
            });
        });
    }

    /**
     * In many dialects of SQL it is valid to sort by attributes that are not present in the SELECT
     * clause.  This rule detects such queries and adds the required attributes to the original
     * projection, so that they will be available during sorting. Another projection is added to
     * remove these attributes after sorting.
     */
    private class ResolveSortReferences implements Rule<LogicalPlan> {
        @Override
        public LogicalPlan apply(LogicalPlan plan) {
            return plan.resolveOperators(p -> {
                if (p instanceof Sort && ((Sort) p).child() instanceof Project) {
                    Sort sort = (Sort) p;
                    Project project = (Project) sort.child();
                    if (!sort.resolved() && sort.child().resolved()) {
                        Pair<List<SortOrder>, Set<Attribute>> pair = resolveAndFindMissing(
                                sort.order,
                                project,
                                project.child());
                        List<SortOrder> newOrdering = pair.first;
                        Set<Attribute> missing = pair.second;

                        // If this rule was not a no-op, return the transformed plan, otherwise return the original.
                        if (!missing.isEmpty()) {
                            List<NamedExpression> newProjectList = concatToList(project.projectList, missing);
                            // Add missing attributes and then project them away after the sort.
                            return new Project(project.projectList,
                                    new Sort(newOrdering, new Project(newProjectList, project.child())));
                        }
                    }
                }
                return p;
            });
        }

        /**
         * Given a child and a grandchild that are present beneath a sort operator, try to resolve
         * the sort ordering and returns it with a list of attributes that are missing from the
         * child but are present in the grandchild.
         */
        private Pair<List<SortOrder>, Set<Attribute>> resolveAndFindMissing(
                List<SortOrder> ordering,
                LogicalPlan child,
                LogicalPlan grandchild) {
            List<SortOrder> newOrdering = resolveSortOrders(ordering, grandchild);
            // Construct a set that contains all of the attributes that we need to evaluate the
            // ordering.
            Set<Attribute> requiredAttributes = filterToSet(Expression.getReferences(newOrdering), Expression::resolved);
            // Figure out which ones are missing from the projection, so that we can add them and
            // remove them after the sort.
            requiredAttributes.removeAll(child.output());
            // It is important to return the new SortOrders here, instead of waiting for the standard
            // resolving process as adding attributes to the project below can actually introduce
            // ambiguity that was not present before.
            return new Pair<>(newOrdering, requiredAttributes);
        }
    }

    /**
     * Turns projections that contain aggregate expressions into aggregations.
     */
    private class GlobalAggregates implements Rule<LogicalPlan> {
        @Override
        public LogicalPlan apply(LogicalPlan oldPlan) {
            return oldPlan.resolveOperators(plan -> {
                if (plan instanceof Project) {
                    Project project = (Project) plan;
                    if (containsAggregates(up(project.projectList))) {
                        return new Aggregate(empty(), project.projectList, project.child());
                    }
                }
                return plan;
            });
        }

        protected boolean containsAggregates(List<Expression> exprs) {
            for (Expression expr : exprs) {
                if (expr.collectFirst(e -> e instanceof AggregateExpression ? e : null) != null) {
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * This rule finds aggregate expressions that are not in an aggregate operator.  For example,
     * those in a HAVING clause or ORDER BY clause.  These expressions are pushed down to the
     * underlying aggregate operator and then projected away after the original operator.
     */
    private class ResolveAggregateFunctions implements Rule<LogicalPlan> {
        @Override
        public LogicalPlan apply(LogicalPlan oldPlan) {
            return oldPlan.resolveOperators(plan -> {
                LogicalPlan newPlan = null;
                if ((plan instanceof Sort) && (newPlan = applySort((Sort) plan)) != null) {
                    return newPlan;
                }
                if ((plan instanceof Aggregate) && (newPlan = applyAggregat((Aggregate) plan)) != null) {
                    return newPlan;
                }
                return plan;
            });
        }

        protected LogicalPlan applySort(Sort sort) {
            if (!(sort.child() instanceof Aggregate) || !sort.child().resolved()) {
                return null;
            }
            if (true) {
                // TODO produce bug
                return null;
            }
            List<SortOrder> sortOrder = sort.order;
            Aggregate aggregate = (Aggregate) sort.child();
            List<SortOrder> unresolvedSortOrders = filterToList(sortOrder, s -> !s.resolved() || containsAggregate(s));
            // List<Alias>
            List<NamedExpression> aliasedOrdering = mapToList(unresolvedSortOrders, o -> new Alias(o.child, "aggOrder"));
            Aggregate aggregatedOrdering = new Aggregate(
                    aggregate.groupingExpressions,
                    aliasedOrdering/*aggregateExpressions*/,
                    aggregate.child());
            Aggregate resolvedAggregate = (Aggregate) execute(aggregatedOrdering);
            List<NamedExpression> resolvedAliasedOrdering = resolvedAggregate.aggregateExpressions;

            assert resolvedAliasedOrdering.size() == unresolvedSortOrders.size();

            // If we pass the analysis check, then the ordering expressions should only reference to
            // aggregate expressions or grouping expressions, and it's safe to push them down to
            // Aggregate.
            if (!resolvedAggregate.resolved()) {
                return null;
            }

            List<NamedExpression> originalAggExprs = mapToList(
                    aggregate.aggregateExpressions,
                    e -> (NamedExpression) trimNonTopLevelAliases(e));

            // If the ordering expression is same with original aggregate expression, we don't need
            // to push down this ordering expression and can reference the original aggregate
            // expression instead.

            List<NamedExpression> needsPushDown = new ArrayList<>();
            Map<SortOrder, SortOrder> sortOrdersMap = new HashMap<>();

            for (int i = 0; i < resolvedAliasedOrdering.size(); i++) {
                Alias evaluated = (Alias) resolvedAliasedOrdering.get(i);
                SortOrder order = unresolvedSortOrders.get(i);
                int index = indexWhere(originalAggExprs, e -> {
                    if (e instanceof Alias) {
                        return ((Alias) e).child.semanticEquals(evaluated.child);
                    } else {
                        return e.semanticEquals(evaluated.child);
                    }
                });
                if (index < 0) {
                    needsPushDown.add(evaluated);
                    sortOrdersMap.put(order, new SortOrder(evaluated.toAttribute(), order.direction));
                } else {
                    //evaluatedOrderings.add(new SortOrder(originalAggExprs.get(index).toAttribute(), order.direction));
                    sortOrdersMap.put(order, new SortOrder(originalAggExprs.get(index).toAttribute(), order.direction));
                }
            }

            List<SortOrder> finalSortOrders = mapToList(sortOrder, o -> {
                return sortOrdersMap.getOrDefault(o, o);
            });

            // Since we don't rely on sort.resolved as the stop condition for this rule,
            // we need to check this and prevent applying this rule multiple times
            if (sortOrder.equals(finalSortOrders)) {
                return sort;
            } else {
                Sort newSort = new Sort(finalSortOrders,
                        new Aggregate(aggregate.groupingExpressions, concatToList(originalAggExprs, needsPushDown), aggregate.child));
                return new Project(up(aggregate.output()), newSort);
            }
        }

        protected LogicalPlan applyAggregat(Aggregate agg) {
            return null;
        }

        protected boolean containsAggregate(Expression condition) {
            return condition.find(e -> e instanceof AggregateExpression) != null;
        }
    }

    static Expression trimAliases(Expression expr) {
        return expr.transformDown(e -> {
            if (e instanceof Alias) {
                return ((Alias) e).child;
            }
            return e;
        });
    }

    static Expression trimNonTopLevelAliases(Expression expr) {
        if (expr instanceof Alias) {
            Alias alias = (Alias) expr;
            Expression newChild = trimAliases(alias.child);
            if (!newChild.fastEquals(alias.child)) {
                return new Alias(trimAliases(alias.child), alias.name(), alias.exprId);
            } else {
                return expr;
            }
        }
        return trimAliases(expr);
    }

    /**
     * Cleans up unnecessary Aliases inside the plan. Basically we only need Alias as a top level
     * expression in Project(project list) or Aggregate(aggregate expressions) or
     * Window(window expressions).
     */
    private class CleanupAliases implements Rule<LogicalPlan> {
        @Override
        public LogicalPlan apply(LogicalPlan oldPlan) {
            return oldPlan.resolveOperators(plan -> {
                if (plan instanceof Project) {
                    Project project = (Project) plan;
                    List<NamedExpression> cleanedProjectList = mapToList(project.projectList, e -> (NamedExpression) trimNonTopLevelAliases(e));
                    if (!compareList(cleanedProjectList, project.projectList, Expression::fastEquals)) {
                        return new Project(cleanedProjectList, project.child());
                    } else {
                        return plan;
                    }
                } else if (plan instanceof Aggregate) {
                    Aggregate aggregate = (Aggregate) plan;
                    List<Expression> cleanedGroupings = mapToList(aggregate.groupingExpressions, Analyzer::trimAliases);
                    List<NamedExpression> cleanedAggs = mapToList(aggregate.aggregateExpressions, e -> (NamedExpression) trimNonTopLevelAliases(e));
                    if (!compareList(cleanedGroupings, aggregate.groupingExpressions, Expression::fastEquals)
                            || !compareList(cleanedAggs, aggregate.aggregateExpressions, Expression::fastEquals)) {
                        return new Aggregate(
                                cleanedGroupings,
                                cleanedAggs,
                                aggregate.child());
                    } else {
                        return plan;
                    }
                }
                return plan.transformExpressionsDown(e -> {
                    if (e instanceof Alias) {
                        return ((Alias) e).child;
                    }
                    return e;
                });
            });
        }
    }


}
