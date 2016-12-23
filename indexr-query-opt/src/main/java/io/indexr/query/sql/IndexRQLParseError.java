package io.indexr.query.sql;

public class IndexRQLParseError extends RuntimeException {
    public IndexRQLParseError() {}

    public IndexRQLParseError(String message) {
        super(message);
    }

    public IndexRQLParseError(String message,
                              Throwable cause) {
        super(message, cause);
    }

    public IndexRQLParseError(Throwable cause) {
        super(cause);
    }

    public IndexRQLParseError(String message,
                              Throwable cause,
                              boolean enableSuppression,
                              boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
