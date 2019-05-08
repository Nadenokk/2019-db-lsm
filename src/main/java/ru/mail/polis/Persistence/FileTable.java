package ru.mail.polis.Persistence;

import org.jetbrains.annotations.NotNull;
import java.util.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.io.File;
import java.io.IOException;

public class FileTable implements Table {
    private final int rows;
    private final long offset;
    private final File file;

    public FileTable(final File file) throws IOException {
        this.file = file;
        final int rowsValue;
        final long offset = file.length() - Long.BYTES;;
        final int s;

        try (FileChannel fc = FileChannel.open(file.toPath(), StandardOpenOption.READ)) {
            assert offset < Integer.MAX_VALUE;
            rowsValue = (int) readLong(fc, offset);
            s=readInt(fc, offset);
        }
        this.rows =  rowsValue;
        this.offset = offset - Long.BYTES * rows - Integer.BYTES - s * Integer.BYTES;
    }

    static void write(final Iterator<Cell> cells, final File to) throws IOException {
        try (FileChannel fc = FileChannel.open(to.toPath(), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE))
        {
            final List<Integer> offsets = new ArrayList<>();
            int offset = 0;
            while (cells.hasNext()) {
                offsets.add(offset);
                final Cell cell = cells.next();
                final ByteBuffer key = cell.getKey();
                final int keySize = cell.getKey().remaining();
                final Value value = cell.getValue();

                //Key
                offset += Integer.BYTES + key.remaining();
                fc.write(fromInt(keySize));
                fc.write(key);

                //Timestamp
                offset += Long.BYTES;
                if (value.isRemoved()) {
                    fc.write(fromLong(-cell.getValue().getTimeStamp()));
                } else {
                    fc.write(fromLong(cell.getValue().getTimeStamp()));
                }

                //Value
                if (!value.isRemoved()) {
                    offset +=Integer.BYTES + value.getData().remaining();
                    final ByteBuffer valueData = value.getData();
                    final int valueSize = valueData.remaining();
                    fc.write(fromInt(valueSize));
                    fc.write(valueData);
                }
            }

            //Offsets
            for (final int anOffset : offsets) {
                fc.write(fromLong(anOffset));
            }
            fc.write(fromInt(0));
            fc.write(fromLong(offsets.size()));
        }
    }

    @NotNull
    private ByteBuffer keyAt(final int i) {
        try (FileChannel fc = FileChannel.open(file.toPath(), StandardOpenOption.READ)) {
            assert fc != null;
            long offset = getOffset(fc, i);
            assert offset <= Integer.MAX_VALUE;

            final int keySize = readInt(fc, offset);
            offset += Integer.BYTES;

            return readBuffer(fc, offset, keySize);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private Cell cellAt(final int i) {
        assert 0 <= i && i < rows;

        try (FileChannel fc = FileChannel.open(file.toPath(), StandardOpenOption.READ)) {
            assert fc != null;
            long offset = getOffset(fc, i);
            assert offset <= Integer.MAX_VALUE;

            //Key
            final int keySize = readInt(fc, offset);
            offset += Integer.BYTES;

            final ByteBuffer key = readBuffer(fc, offset, keySize);
            offset += keySize;

            //Timestamp
            final long timeStamp = readLong(fc, offset);
            offset += Long.BYTES;

            if (timeStamp < 0) {
                return new Cell(key, Value.tombstone(-timeStamp));
            }

            final int valueSize = readInt(fc, offset);
            offset += Integer.BYTES;

            final ByteBuffer value = readBuffer(fc, offset, valueSize);
            return new Cell(key, Value.of(timeStamp, value));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private int position(final ByteBuffer from) {
        int left = 0;
        int right = rows - 1;
        while (left <= right) {
            final int mid = left + ((right - left) >> 1);
            final int cmp = from.compareTo(keyAt(mid));
            if (cmp < 0) {
                right = mid - 1;
            } else if (cmp > 0) {
                left = mid + 1;
            } else {
                return mid;
            }
        }
        return left;
    }

    @Override
    public long sizeInBytes() {
        throw new UnsupportedOperationException("");
    }

    @NotNull
    @Override
    public Iterator<Cell> iterator(@NotNull final ByteBuffer from) {
        return new Iterator<>() {
            int next = position(from);

            @Override
            public boolean hasNext() {
                return next < rows;
            }

            @Override
            public Cell next() {
                assert hasNext();
                return cellAt(next++);
            }
        };
    }

    private int readInt(final FileChannel fc, final long offset) {
        final ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        try {
            fc.read(buffer, offset);
            return buffer.rewind().getInt();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return -1;
    }

    private long readLong(final FileChannel fc, final long offset) {
        final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        try {
            fc.read(buffer, offset);
            return buffer.rewind().getLong();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return -1L;
    }

    private ByteBuffer readBuffer(final FileChannel fc, final long offset, final int size) {
        final ByteBuffer buffer = ByteBuffer.allocate(size);
        try {
            fc.read(buffer, offset);
            return buffer.rewind();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return buffer;
    }

    private long getOffset(final FileChannel fc, final int i) {
        final ByteBuffer offsetBB = ByteBuffer.allocate(Long.BYTES);
        try {
            fc.read(offsetBB, offset + Long.BYTES * i);

        } catch (IOException e) {
            e.printStackTrace();
        }
        return offsetBB.rewind().getLong();
    }


    public static ByteBuffer fromInt(final int value){
        return ByteBuffer.allocate(Integer.BYTES).putInt(value).rewind();
    }

    public static ByteBuffer fromLong(final long value) {
        return ByteBuffer.allocate(Long.BYTES).putLong(value).rewind();
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) {
        throw new UnsupportedOperationException("");
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) {
        throw new UnsupportedOperationException("");
    }
}

