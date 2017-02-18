package io.indexr.segment;

public interface PackRSIndexNum extends PackRSIndex {
    void putValue(long value, long packMin, long packMax);

    byte isValue(long minVal, long maxVal, long packMin, long packMax);
}
