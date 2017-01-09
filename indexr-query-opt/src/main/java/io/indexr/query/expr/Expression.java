package io.indexr.query.expr;

import org.apache.commons.lang.StringUtils;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Collectors;

import io.indexr.query.TreeNode;
import io.indexr.query.expr.attr.Attribute;
import io.indexr.query.expr.attr.AttributeReference;
import io.indexr.query.expr.attr.PrettyAttribute;
import io.indexr.query.expr.attr.UnresolvedAttribute;

import static io.indexr.util.Trick.flatMapToList;

public abstract class Expression extends TreeNode<Expression> implements Evaluable {
    /**
     * Returns true when an expression is a candidate for static evaluation before the query is
     * executed.
     * 
     * The following conditions are used to determine suitability for constant folding:
     * - A [[Coalesce]] is foldable if all of its children are foldable
     * - A [[BinaryExpression]] is foldable if its both left and right child are foldable
     * - A [[Not]], [[IsNull]], or [[IsNotNull]] is foldable if its child is foldable
     * - A [[Literal]] is foldable
     * - A [[Cast]] or [[UnaryMinus]] is foldable if its child is foldable
     */
    public boolean foldable() {
        return false;
    }

    public Set<Attribute> references() {
        return children().stream().flatMap(e -> e.references().stream()).collect(Collectors.toSet());
    }

    public boolean resolved() {return childrenResolved() && checkInputDataTypes().isSuccess();}

    public boolean childrenResolved() {
        for (Expression e : children()) {
            if (!e.resolved()) return false;
        }
        return true;
    }

    private boolean checkSemantic(Collection<Object> elements1, Collection<Object> elements2) {
        if (elements1.size() != elements2.size()) {
            return false;
        }
        Iterator it1 = elements1.iterator();
        Iterator it2 = elements2.iterator();
        while (it1.hasNext()) {
            Object e1 = it1.next();
            Object e2 = it2.next();
            if (e1 == e2) {
                continue;
            }
            if (e1 == null || e2 == null) {
                return false;
            }
            if (e1 instanceof Expression && e2 instanceof Expression) {
                if (!((Expression) e1).semanticEquals((Expression) e2)) {
                    return false;
                }
            }
            if (e1 instanceof Collection && e2 instanceof Collection) {
                if (!checkSemantic((Collection) e1, (Collection) e2)) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Returns true when two expressions will always compute the same result, even if they differ
     * cosmetically (i.e. capitalization of names in attributes may be different).
     */
    public boolean semanticEquals(Expression other) {
        return this.getClass() == other.getClass() && checkSemantic(this.args(), other.args());
    }

    public TypeCheckResult checkInputDataTypes() {return TypeCheckResult.success();}

    public static Set<Attribute> getReferences(Iterable<? extends Expression> exprs) {
        HashSet<Attribute> refs = new HashSet<>();
        for (Expression e : exprs) {
            refs.addAll(e.references());
        }
        return refs;
    }

    /**
     * Returns a user-facing string representation of this expression's name.
     * This should usually match the name of the function in SQL.
     */
    public String prettyName() {
        return getClass().getSimpleName().toLowerCase();
    }

    /**
     * Returns a user-facing string representation of this expression, i.e. does not have developer
     * centric debugging information like the expression id.
     */
    public String prettyString() {
        return transform(a -> {
            if (a instanceof AttributeReference) {
                AttributeReference attr = (AttributeReference) a;
                return new PrettyAttribute(attr.name(), attr.dataType);
            }
            if (a instanceof UnresolvedAttribute) {
                UnresolvedAttribute attr = (UnresolvedAttribute) a;
                return new PrettyAttribute(attr.name());
            }
            return a;
        }).toString();
    }


    Collection<Object> flatArguments() {
        return flatMapToList(args(), a -> {
            if (a instanceof Collection) {
                return (Collection) a;
            }
            return Collections.singleton(a);
        });
    }

    @Override
    public String simpleString() {
        return toString();
    }

    @Override
    public String toString() {
        return prettyName() + "(" + StringUtils.join(flatArguments(), ",") + ")";
    }

}
