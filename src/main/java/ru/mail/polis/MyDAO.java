package ru.mail.polis;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

public class MyDAO implements DAO{

    private final SortedMap<ByteBuffer, Record> data = new TreeMap<>();

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) {
        return data.tailMap(from).values().iterator();
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) {
        data.put(key, Record.of(key, value));
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) {
        data.remove(key);
    }

    @Override
    public void close() throws IOException {
        // do nothing
    }
}