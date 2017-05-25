package io.indexr.data;

import java.io.IOException;

@FunctionalInterface
public interface Cleanable {
    void clean() throws IOException;
}
