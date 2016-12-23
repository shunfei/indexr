package io.indexr.query;

@FunctionalInterface
public interface Rule<TreeType extends TreeNode<TreeType>> {
    TreeType apply(TreeType plan);

    /** Name for this rule, automatically inferred based on class name. */
    default String ruleName() {
        String className = this.getClass().getName();
        return className.endsWith("$")
                ? className.substring(className.length() - 1)
                : className;
    }
}
