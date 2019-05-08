package ru.mail.polis.Persistence;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import org.jetbrains.annotations.NotNull;

public final class Value implements Comparable<Value> {
    private final long ts;
    private final ByteBuffer data;
    private static final AtomicInteger atomicInteger = new AtomicInteger();

    /**
     * Represent the data with timestamps.
     *
     * @param ts   timestamp
     * @param data any data for DB
     */
    private Value(final long ts, final ByteBuffer data) {
        assert ts >= 0;
        this.ts = ts;
        this.data = data;
    }

    public static Value of(final ByteBuffer data) {
        return new Value(getTime(), data.duplicate());
    }

    public static Value of(final long time, final ByteBuffer data) {
        return new Value(time, data.duplicate());
    }

    public static Value tombstone() {
        return tombstone(getTime());
    }

    public static Value tombstone(final long time) {
        return new Value(time, null);
    }

    public boolean isRemoved() {
        return data == null;
    }

    /**
     * Method for get data from DB.
     *
     * @return data from DB
     */
    public ByteBuffer getData() {
        if (data == null) {
            throw new IllegalArgumentException("");
        }
        return data.asReadOnlyBuffer();
    }

    @Override
    public int compareTo(@NotNull final Value o) {
        return Long.compare(o.ts, ts);
    }

    long getTimeStamp() {
        return ts;
    }

    private static long getTime() {
        final long time = System.currentTimeMillis() * 1000 + atomicInteger.incrementAndGet();
        if (atomicInteger.get() > 1000) atomicInteger.set(0);
        return time;
    }
}
