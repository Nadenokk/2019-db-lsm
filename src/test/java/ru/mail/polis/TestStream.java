/*
 * Copyright 2018 (c) Vadim Tsesko <incubos@yandex.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ru.mail.polis;

import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.NoSuchElementException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.io.OutputStream;
import java.io.InputStream;
import java.io.FileOutputStream;
import java.io.FileInputStream;

import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

/**
 * Contains utility methods for unit tests.
 *
 * @author Vadim Tsesko and Naydennok
 */
public class TestStream {
    static final int KEY_LENGTH = 16;
    private static final int VALUE_LENGTH = 1024;
    private static final String INSUFFIX = ".Indat";
    private static final String OUTSUFFIX = ".Outtmp";
    private static final String TABLE = "ssTable";

    @Test
    void testStream(@TempDir File data) throws IOException {
        // Reference value
        final int valueSize = 1024 * 1024;
        final int keyCount = 16;

        final ByteBuffer value = ByteBuffer.allocateDirect(valueSize);
        for (int i = 0; i < valueSize; i++) {
            final byte[] result = new byte[1];
            ThreadLocalRandom.current().nextBytes(result);
            value.put(i, result[0]);
        }

        final Collection<ByteBuffer> keys = new ArrayList<>(keyCount);
        for (int i = 0; i < keyCount; i++) {
            final ByteBuffer key = ByteBuffer.allocateDirect(keyCount);
            for (int j = 0; j < keyCount; j++) {
                final byte[] result = new byte[1];
                ThreadLocalRandom.current().nextBytes(result);
                value.put(j, result[0]);
            }
            keys.add(key);
        }
        // Insert keys
        try (DAO dao = DAOFactory.create(data)) {
            for (final ByteBuffer key : keys) {
                final ByteBuffer result = ByteBuffer.allocateDirect(key.remaining() + value.remaining());
                result.put(key.duplicate());
                result.put(value.duplicate());
                result.rewind();
                ByteBuffer tmp = result.duplicate();

                OutputStream outputStream = null;
                String str = key.toString();
                File file = new File(data, str + INSUFFIX);
                try {
                    outputStream = new FileOutputStream(file);
                    final WritableByteChannel channel = Channels.newChannel(outputStream);
                    channel.write(tmp);
                    channel.close();
                } finally {
                    if (outputStream != null) {
                        outputStream.close();
                    }
                }
                File initialFile = new File(data, str + INSUFFIX);
                InputStream inputStream = new FileInputStream(initialFile);
                dao.upsertStream(key, inputStream);
                inputStream.close();
                file.delete();
            }
        }

        // Check the contents
        try (DAO dao = DAOFactory.create(data)) {
            for (final ByteBuffer key : keys) {
                final ByteBuffer result = ByteBuffer.allocateDirect(key.remaining() + value.remaining());
                result.put(key.duplicate());
                result.put(value.duplicate());
                result.rewind();
                result.flip();

                OutputStream outputStream = null;
                String str = key.toString();
                File file = new File(data, str + OUTSUFFIX);
                try {
                    outputStream = new FileOutputStream(file);
                    dao.getStream(key, outputStream);

                } finally {
                    if (outputStream != null) {
                        outputStream.close();
                    }
                }


                InputStream inputStream = new FileInputStream(file);
                final ByteBuffer valueTmp = ByteBuffer.allocateDirect(inputStream.available());
                valueTmp.clear();
                int curent = inputStream.available();
                final byte[] bytes = new byte[1024];
                while ((inputStream.read(bytes)) != -1) {
                    final int shiftBuf = Math.min(curent, bytes.length);
                    for (int i = 0; i < shiftBuf; i++) {
                        valueTmp.put(bytes[i]);
                    }
                    curent = curent - shiftBuf;
                }
                value.flip();
                inputStream.close();
                file.delete();
                assertEquals(result, valueTmp);
            }


            // Remove keys
            for (final ByteBuffer key : keys) {
                dao.remove(key);
            }
        }

        // Compact
        try (DAO dao = DAOFactory.create(data)) {
            dao.compact();
        }

        // Check the contents
        try (DAO dao = DAOFactory.create(data)) {
            for (final ByteBuffer key : keys) {
                assertThrows(NoSuchElementException.class, () -> dao.get(key));
            }
        }

        // Check store size
        final long size = Files.directorySize(data);

        // Heuristic
        assertTrue(size < valueSize);
    }
}
