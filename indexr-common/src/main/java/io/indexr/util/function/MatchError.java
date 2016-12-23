package io.indexr.util.function;


/**
 * This class implements errors which are thrown whenever an
 * object doesn't match any pattern of a pattern matching
 * expression.
 */
public class MatchError extends RuntimeException {
    public MatchError(Object obj) {
        super(obj == null ? "null" : obj.toString() + " (of class" + obj.getClass().getName() + ")");
    }
}
