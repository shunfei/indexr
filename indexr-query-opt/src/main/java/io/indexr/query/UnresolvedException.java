package io.indexr.query;

public class UnresolvedException extends RuntimeException {
    public UnresolvedException(TreeNode tree, String function) {
        this(String.format("Invalid call to %s on unresolved object", function));
    }

    public UnresolvedException() {
    }

    public UnresolvedException(String message) {
        super(message);
    }

    public UnresolvedException(String message, Throwable cause) {
        super(message, cause);
    }

    public UnresolvedException(Throwable cause) {
        super(cause);
    }

    public UnresolvedException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
