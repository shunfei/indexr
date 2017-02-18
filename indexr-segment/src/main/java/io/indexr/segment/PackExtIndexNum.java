package io.indexr.segment;

public interface PackExtIndexNum extends PackExtIndex {
    void putValue(int rowId, long val);

    byte isValue(int rowId, long val);
}
