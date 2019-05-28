package ru.mail.polis.persistence;

import org.jetbrains.annotations.NotNull;
import java.util.concurrent.atomic.AtomicInteger;

import java.nio.ByteBuffer;

public final class Value implements Comparable<Value> {
    private final long ts;
    private final long ttl;
    private final ByteBuffer data;
    private static final AtomicInteger atomicInteger = new AtomicInteger();

    Value(final long ts, final ByteBuffer data) {
        this.ts = ts;
        this.data = data;
        this.ttl =0;
    }

    Value(final long ts, final ByteBuffer data,final long ttl) {
        this.ts = ts;
        this.data = data;
        this.ttl =ttl;
    }

    public static Value of(final ByteBuffer data,final long ttl) {
        return new Value(getTime(), data.duplicate(),ttl);
    }

    static Value tombstone() {
        return new Value(getTime(), null);
    }

    boolean isRemoved() {
        if (data == null){
            return true;
        }else if (ttl>0){
            if (getTime() > ts + ttl){
                return true;
            }
        }
        return false;
    }

    ByteBuffer getData() {
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
        final long time = System.currentTimeMillis() * 10000 + atomicInteger.incrementAndGet();
        if (atomicInteger.get() > 10000) atomicInteger.set(0);
        return time;
    }
}
