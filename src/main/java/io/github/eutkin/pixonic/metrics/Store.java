package io.github.eutkin.pixonic.metrics;

import java.io.Closeable;

public interface Store extends Closeable {

    void add(long timestamp, char key, int value);

    long sum(long startTimestampInclusive, long endTimestampExclusive, char key);
}
