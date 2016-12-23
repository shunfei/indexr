package io.indexr.data;

public interface ValueSetter extends IntSetter, LongSetter, FloatSetter, DoubleSetter, BytePieceSetter {
    @Override
    default void set(int id, BytePiece bytes) {}

    @Override
    default void set(int id, double value) {}

    @Override
    default void set(int id, float value) {}

    @Override
    default void set(int id, int value) {}

    @Override
    default void set(int id, long value) {}
}
