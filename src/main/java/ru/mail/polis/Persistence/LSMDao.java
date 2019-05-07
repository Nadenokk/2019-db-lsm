package ru.mail.polis.Persistence;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.DAO;
import ru.mail.polis.Iters;
import ru.mail.polis.Record;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.StandardCopyOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;


public class LSMDao implements DAO {
    private static final String TABLE_NAME = "SSTable";
    private static final String SUFFIX = ".dat";
    private static final String TEMP = ".tmp";
    private final Table memTable = new MemTable();
    private final long flushThreshold;
    private final File base;
    private int currentGeneration;
    private List<Table> fileTables;

    public LSMDao(final File base, final long flushThreshold) throws IOException {
        this.base = base;
        assert flushThreshold >= 0L;
        this.flushThreshold = flushThreshold;
        readFiles();
    }

    private void readFiles() throws IOException {
        try (Stream<Path> stream = Files.walk(base.toPath(), 1).filter(path -> {
                    final String fileName = path.getFileName().toString();
                    return fileName.endsWith(SUFFIX) && fileName.contains(TABLE_NAME);
                })) {
            final List<Path> files = stream.collect(Collectors.toList());
            fileTables = new ArrayList<>(files.size());
            currentGeneration = -1;
            files.forEach(path -> {
                final File file = path.toFile();
                try {
                    final FileTable fileChannelTable = new FileTable(file);
                    fileTables.add(fileChannelTable);
                    currentGeneration = Math.max(currentGeneration,
                            FileTable.getGenerationByName(file.getName()));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            currentGeneration++;
        }
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        final List<Iterator<Cell>> list = new ArrayList<>(fileTables.size() + 1);
        for (final Table fileChannelTable : fileTables) {
            list.add(fileChannelTable.iterator(from));
        }
        final Iterator<Cell> memoryIterator = memTable.iterator(from);
        list.add(memoryIterator);
        final Iterator<Cell> iterator = Iters.collapseEquals(Iterators.mergeSorted(list, Cell.COMPARATOR));
        final Iterator<Cell> alive = Iterators.filter(iterator, cell -> !cell.getValue().isRemoved());
        return Iterators.transform(alive, cell -> Record.of(cell.getKey(), cell.getValue().getData()));
    }

    private void updateData() throws IOException {
        if (memTable.sizeInBytes() > flushThreshold) {
            flush();
        }
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        memTable.upsert(key.duplicate(), value);
        updateData();
    }

    private void flush() throws IOException {
        currentGeneration++;
        final File tmp = new File(base, currentGeneration + TABLE_NAME + TEMP);
        FileTable.write(memTable.iterator(ByteBuffer.allocate(0)), tmp);
        final File dest = new File(base, currentGeneration + TABLE_NAME + SUFFIX);
        Files.move(tmp.toPath(), dest.toPath(), StandardCopyOption.ATOMIC_MOVE);
        fileTables.add(new FileTable(dest));
        memTable.clear();
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        memTable.remove(key);
        updateData();
    }

    @NotNull
    @Override
    public ByteBuffer get(@NotNull final ByteBuffer key) throws IOException, NoSuchElementException {
        final Cell memCell = memTable.get(key);

        if (memCell != null) {
            if (memCell.getValue().isRemoved()) {
                throw new NoSuchElementException("");
            }
            return memCell.getValue().getData();
        }

        final ConcurrentLinkedQueue<Cell> cells = new ConcurrentLinkedQueue<>();
        final AtomicInteger counter = new AtomicInteger(0);
        fileTables.forEach(table -> new Thread(() -> {
            try {
                final Cell cell = table.get(key);
                if (cell != null) {
                    cells.add(cell);
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                counter.incrementAndGet();
            }
        }).start());

        while (counter.get() < fileTables.size()) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        if (cells.size() == 0) {
            throw new NoSuchElementException("");
        }

        final Cell cell = Collections.min(cells, Cell.COMPARATOR);
        if (cell == null || cell.getValue().isRemoved()) {
            throw new NoSuchElementException("");
        }
        final Record record = Record.of(cell.getKey(), cell.getValue().getData());
        return record.getValue();
    }

    @Override
    public void close() throws IOException {
        flush();
    }
}