package io.github.eutkin.pixonic.metrics;

public class Input implements Comparable<Input> {

    public final long timestamp;
    public final char key;
    public final int value;


    public Input(long timestamp, char key, int value) {
        this.timestamp = timestamp;
        this.key = key;
        this.value = value;
    }

    @Override
    public String toString() {
        return "Input{" +
                "timestamp=" + timestamp +
                ", key=" + key +
                ", value=" + value +
                '}';
    }

    @Override
    public int compareTo(Input o) {
        return Long.compare(timestamp, o.timestamp);
    }
}
