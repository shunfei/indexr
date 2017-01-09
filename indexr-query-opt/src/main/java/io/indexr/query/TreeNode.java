package io.indexr.query;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import io.indexr.util.Reflection;
import io.indexr.util.Wrapper;
import io.indexr.util.function.PartialFunction;

import static io.indexr.util.Lazily.lazily;
import static io.indexr.util.Lazily.value;
import static io.indexr.util.Trick.concatToList;
import static io.indexr.util.Trick.identity;
import static io.indexr.util.Trick.mapToList;

/**
 * Should ensure that if we pass the child list returned from {@link #children()} to {@link #withNewChildren(List)},
 * the new TreeNode should work exactly the same as the old one.
 */
@SuppressWarnings("unchecked")
public abstract class TreeNode<BaseType extends TreeNode<BaseType>> {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public Supplier<Set<BaseType>> childSet = lazily(() -> childSet = value(new HashSet<>(children())));

    public abstract List<BaseType> children();

    public boolean containsChild(TreeNode child) {
        return childSet.get().contains(child);
    }

    /**
     * The implementation should return a new instance other than `this`.
     */
    public abstract BaseType withNewChildren(List<BaseType> newChildren);

    public abstract List<Object> args();

    public BaseType withNewArgs(List<Object> args) {
        Class<?>[] argTypes = new Class<?>[args.size()];
        int i = 0;
        for (; i < argTypes.length; i++) {
            argTypes[i] = args.get(i).getClass();
        }
        Object[] argList = args.toArray();
        try {
            return (BaseType) Reflection.getConstructor(this.getClass(), argList).newInstance(argList);
            //return (BaseType) this.getClass().getConstructor(argTypes).newInstance(args.toArray());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Faster version of equality which short-circuits when two treeNodes are the same instance.
     * We don't just override Object.equals, as doing so prevents the scala compiler from
     * generating case class `equals` methods
     */
    public boolean fastEquals(TreeNode other) {
        return this == other || this.equals(other);
    }

    /**
     * Find the first [[TreeNode]] that satisfies the condition specified by `f`.
     * The condition is recursively applied to this node and all of its children (pre-order).
     */
    public BaseType find(Predicate<? super BaseType> p) {
        if (p.test((BaseType) this)) {
            return (BaseType) this;
        }
        for (BaseType c : children()) {
            if (c.find(p) != null) {
                return c;
            }
        }
        return null;
    }

    /**
     * Runs the given function on this node and then recursively on [[children]].
     *
     * @param f the function to be applied to each node in the tree.
     */
    public void foreach(Consumer<? super BaseType> f) {
        f.accept((BaseType) this);
        children().forEach(c -> c.foreach(f));
    }

    /**
     * Runs the given function recursively on [[children]] then on this node.
     *
     * @param f the function to be applied to each node in the tree.
     */
    public void foreachUp(Consumer<? super BaseType> f) {
        f.accept((BaseType) this);
        children().forEach(c -> c.foreach(f));
    }

    /**
     * Returns a Seq containing the result of applying the given function to each
     * node in this tree in a preorder traversal.
     *
     * @param f the function to be applied.
     */
    public <A> List<A> map(Function<? super BaseType, ? extends A> f) {
        List<A> all = new ArrayList<>();
        foreach(t -> {
            all.add(f.apply(t));
        });
        return all;
    }

    /**
     * Returns a Seq by applying a function to all nodes in this tree and using the elements of the
     * resulting collections.
     */
    public <A> List<A> flatMap(Function<? super BaseType, ? extends Collection<? extends A>> f) {
        List<A> all = new ArrayList<>();
        foreach(t -> {
            all.addAll(f.apply(t));
        });
        return all;
    }

    /**
     * Collect the elements produced by `f` and not null.
     */
    public <A> List<A> collect(Function<BaseType, A> f) {
        ArrayList<A> list = new ArrayList<>();
        this.foreach(node -> {
            A res = f.apply(node);
            if (res != null) list.add(res);
        });
        return list;
    }

    public <A> A collectFirst(Function<BaseType, A> f) {
        Wrapper wrapper = new Wrapper();
        this.foreach(node -> {
            if (wrapper.value == null) {
                A res = f.apply(node);
                if (res != null) {
                    wrapper.value = res;
                }
            }
        });
        return (A) wrapper.value;
    }

    public BaseType transform(Function<BaseType, BaseType> rule) {
        return transform(PartialFunction.fromFunction(rule));
    }

    public BaseType transform(PartialFunction<BaseType, BaseType> rule) {
        return transformDown(rule);
    }

    public BaseType transformDown(PartialFunction<BaseType, BaseType> rule) {
        BaseType afterRule = rule.applyOrElse((BaseType) this, identity());
        if (this.fastEquals(afterRule)) {
            return transformChildren(rule, TreeNode::transformDown);
        } else {
            return afterRule.transformChildren(rule, TreeNode::transformDown);
        }
    }

    public BaseType transformDown(Function<BaseType, BaseType> rule) {
        return transformDown(PartialFunction.fromFunction(rule));
    }

    public BaseType transformUp(PartialFunction<BaseType, BaseType> rule) {
        BaseType afterRuleOnChildren = transformChildren(rule, TreeNode::transformUp);
        if (this.fastEquals(afterRuleOnChildren)) {
            return rule.applyOrElse((BaseType) this, identity());
        } else {
            return rule.applyOrElse(afterRuleOnChildren, identity());
        }
    }

    public BaseType transformUp(Function<BaseType, BaseType> rule) {
        return transformUp(PartialFunction.fromFunction(rule));
    }

    protected BaseType transformChildren(PartialFunction<BaseType, BaseType> f,
                                         BiFunction<BaseType, PartialFunction<BaseType, BaseType>, BaseType> nextOperation) {
        ArrayList<BaseType> newChildren = new ArrayList<>();
        boolean updated = false;
        for (BaseType child : children()) {
            BaseType newChild = nextOperation.apply(child, f);
            if (!child.fastEquals(newChild)) {
                updated = true;
            }
            if (newChild != null) {
                newChildren.add(newChild);
            }
        }
        if (updated) {
            return withNewChildren(newChildren);
        } else {
            return (BaseType) this;
        }
    }

    /** Returns the name of this type of TreeNode.  Defaults to the class name. */
    public String nodeName() {
        return getClass().getSimpleName();
    }

    /** Returns a string representing the arguments to this node, minus any children */
    public String argString() {
        return StringUtils.join(mapToList(args(), arg -> {
            if (arg instanceof TreeNode) {
                if (containsChild((TreeNode) arg)) {
                    return null;
                }
                return ((TreeNode) arg).simpleString();
            }
            if (arg instanceof Collection) {
                if (childSet.get().containsAll((Collection) arg)) {
                    return null;
                }
                if (arg instanceof List) {
                    return "[" + StringUtils.join((List) arg, ", ") + "]";
                }
                if (arg instanceof Set) {
                    return "{" + StringUtils.join((Set) arg, ", ") + "}";
                }
            }
            return arg;
        }, /*ignoreNull*/true), ", ");
    }

    /** STRING representation of this node without any children */
    public String simpleString() {
        return (nodeName() + " " + argString()).trim();
    }

    @Override
    public String toString() {
        return treeString();
    }

    /** Returns a string representation of the nodes in this tree */
    public String treeString() {
        return generateTreeString(0, Collections.emptyList(), new StringBuilder()).toString();
    }

    /**
     * Appends the string represent of this node and its children to the given StringBuilder.
     * 
     * The `i`-th element in `lastChildren` indicates whether the ancestor of the current node at
     * depth `i + 1` is the last child of its own parent node.  The depth of the root node is 0, and
     * `lastChildren` for the root node should be empty.
     */
    StringBuilder generateTreeString(
            int depth, List<Boolean> lastChildren, StringBuilder builder) {
        if (depth > 0) {
            lastChildren.subList(0, lastChildren.size() - 1).forEach(isLast -> {
                String prefixFragment = isLast ? "   " : ":  ";
                builder.append(prefixFragment);
            });

            String branch = lastChildren.get(lastChildren.size() - 1) ? "+- " : ":- ";
            builder.append(branch);
        }

        builder.append(simpleString());
        builder.append("\n");

        List<BaseType> children = children();
        if (!children.isEmpty()) {
            children.subList(0, children.size() - 1).forEach(c -> {
                c.generateTreeString(
                        depth + 1,
                        concatToList(lastChildren, false),
                        builder);
            });
            children.get(children.size() - 1).generateTreeString(
                    depth + 1,
                    concatToList(lastChildren, false),
                    builder);
        }

        return builder;
    }
}
