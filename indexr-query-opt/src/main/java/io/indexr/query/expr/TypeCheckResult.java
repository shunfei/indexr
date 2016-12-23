package io.indexr.query.expr;

public final class TypeCheckResult {
    public final String msg;
    public final boolean isSuccess;

    public TypeCheckResult(String msg, boolean isSuccess) {
        this.msg = msg;
        this.isSuccess = isSuccess;
    }

    public boolean isFailure() {return !isSuccess;}

    public boolean isSuccess() {return isSuccess;}

    private final static TypeCheckResult SuccessResult = new TypeCheckResult(null, true);

    public static TypeCheckResult success() {
        return SuccessResult;
    }

    public static TypeCheckResult fail(String msg) {
        return new TypeCheckResult(msg, false);
    }

    public static TypeCheckResult fail() {
        return fail(null);
    }
}
