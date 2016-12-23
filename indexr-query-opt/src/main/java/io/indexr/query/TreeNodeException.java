package io.indexr.query;

public class TreeNodeException extends RuntimeException {
    private TreeNode tree;

    public TreeNodeException(TreeNode tree, String message, Throwable cause) {
        super(message, cause);
        this.tree = tree;
    }

    public TreeNodeException(TreeNode tree, String message) {
        this(tree, message, null);
    }

    @Override
    public String getMessage() {
        return String.format("%s, tree: %s", super.getMessage(), tree.toString());
    }
}
