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
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class LSMDao implements DAO {
    private static final String SUFFIX = ".dat";
    private static final String TEMP = ".tmp";
    private static final String TABLE_NAME = "ssTable";

    private Table memTable = new MemTable();
    private final long flushThreshold;
    private final File base;
    private int numberGeneration = -1;
    private final List<Table> fileTables;

    /**
     * My NoSQL DAO.
     *
     * @param base           directory of DB
     * @param flushThreshold maxsize of @memTable
     * @throws IOException If an IO error occurs
     */
    public LSMDao(final File base, final long flushThreshold) throws IOException {

        assert flushThreshold >= 0L;
        this.flushThreshold = flushThreshold;
        this.base = base;
        fileTables = new ArrayList<>();

        try (Stream<Path> pStream = Files.walk(base.toPath(), 1).filter(p -> p.getFileName().toString().endsWith(SUFFIX))) {
            pStream.collect(Collectors.toList()).forEach(path -> {
                final File file = path.toFile();
                try {
                    fileTables.add(new FileTable(file));
                    String[] k = file.getName().split(TABLE_NAME);
                    int i = Integer.parseInt(k[0]);
                    numberGeneration = Math.max(numberGeneration,i);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            numberGeneration++;
        }
    }

    private void flush() throws IOException {
        numberGeneration++;
        final File tmp = new File(base, numberGeneration + TABLE_NAME + TEMP);
        FileTable.write(memTable.iterator(ByteBuffer.allocate(0)), tmp);
        final File dest = new File(base, numberGeneration + TABLE_NAME + SUFFIX);
        Files.move(tmp.toPath(), dest.toPath(), StandardCopyOption.ATOMIC_MOVE);
        fileTables.add(new FileTable(dest));
        memTable = new MemTable();
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        final ArrayList<Iterator<Cell>> arrayList = new ArrayList<>(fileTables.size() + 1);
        for (final Table fileChannelTable : fileTables) {
            arrayList.add(fileChannelTable.iterator(from));
        }
        arrayList.add(memTable.iterator(from));
        final Iterator<Cell> iterator = Iters.collapseEquals(Iterators.mergeSorted(arrayList, Cell.COMPARATOR));
        final Iterator<Cell> alive = Iterators.filter(iterator, cell -> !cell.getValue().isRemoved());
        return Iterators.transform(alive, cell -> Record.of(cell.getKey(), cell.getValue().getData()));
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        memTable.upsert(key.duplicate(), value);
        if (memTable.sizeInBytes() > flushThreshold) flush();
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        memTable.remove(key);
        if (memTable.sizeInBytes() > flushThreshold) flush();
    }

    @Override
    public void close() throws IOException {
        flush();
    }
}