/*
 * StreamFrame
 * Copyright (C) 2026 HopesDreams
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
package io.github.hopesdreams.streamframe;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(10)
class SocketFrameTest {

    @Test
    void writesAndParsesWireFormat() {
        long streamId = 0x01020304L;
        long sequence = 0x05060708L;
        SocketFrame frame = SocketFrame.data(streamId, sequence, "abc");
        byte[] bytes = frame.getBytes();

        assertEquals(SocketFrame.FIXED_LENGTH + 3, bytes.length);
        assertEquals(0xAA, bytes[0] & 0xFF);
        assertEquals(0xBB, bytes[1] & 0xFF);
        assertEquals(0x00, bytes[2] & 0xFF);
        assertEquals(0x03, bytes[3] & 0xFF);
        assertEquals(0x00, bytes[4] & 0xFF);
        assertEquals(SocketFrame.TYPE_DATA, bytes[13] & 0xFF);
        assertEquals(streamId, readUnsignedInt(bytes, 14));
        assertEquals(sequence, readUnsignedInt(bytes, 18));
        assertEquals(3, readUnsignedInt(bytes, 22));
        assertArrayEquals("abc".getBytes(StandardCharsets.UTF_8),
                Arrays.copyOfRange(bytes, SocketFrame.PREFIX_LENGTH, bytes.length - 2));
        assertEquals(0xFF, bytes[bytes.length - 2] & 0xFF);
        assertEquals(0xEE, bytes[bytes.length - 1] & 0xFF);

        SocketFrame parsed = new SocketFrame(bytes);
        assertEquals(SocketFrame.TYPE_DATA, parsed.getType());
        assertEquals(streamId, parsed.getStreamId());
        assertEquals(sequence, parsed.getSequence());
        assertEquals("abc", parsed.getPayloadAsString());
        assertEquals(3, parsed.getPayloadLength());
        assertArrayEquals(bytes, parsed.getBytes());
    }

    @Test
    void supportsAllFrameTypes() {
        SocketFrame request = SocketFrame.request(7, 0, "hi");
        assertEquals(SocketFrame.TYPE_REQUEST, request.getType());
        assertEquals("hi", request.getPayloadAsString());

        SocketFrame data = SocketFrame.data(7, 1, new byte[]{0x01});
        assertEquals(SocketFrame.TYPE_DATA, data.getType());

        SocketFrame end = SocketFrame.end(7, 2);
        assertEquals(SocketFrame.TYPE_END, end.getType());
        assertEquals(0, end.getPayloadLength());
        assertEquals(SocketFrame.FIXED_LENGTH, end.getBytes().length);

        SocketFrame error = SocketFrame.error(7, 3, "boom");
        assertEquals(SocketFrame.TYPE_ERROR, error.getType());
        assertEquals("boom", error.getPayloadAsString());

        for (SocketFrame frame : new SocketFrame[]{request, data, end, error}) {
            SocketFrame parsed = new SocketFrame(frame.getBytes());
            assertEquals(frame.getType(), parsed.getType());
            assertEquals(frame.getStreamId(), parsed.getStreamId());
            assertEquals(frame.getSequence(), parsed.getSequence());
        }
    }

    @Test
    void metadataMayContainTailAndHeaderBytes() throws IOException {
        long streamId = 0x00FFEEBBL;
        long sequence = 0xAAFFEEAAL;
        SocketFrame frame = SocketFrame.data(streamId, sequence, "x");
        byte[] bytes = frame.getBytes();
        assertTrue(containsPair(bytes, 0xFF, 0xEE), "metadata should contain raw tail bytes");

        SocketFrame parsed = SocketFrame.readFrom(new ByteArrayInputStream(bytes));
        assertNotNull(parsed);
        assertEquals(streamId, parsed.getStreamId());
        assertEquals(sequence, parsed.getSequence());
        assertEquals("x", parsed.getPayloadAsString());

        SocketFrame.FrameReader reader = SocketFrame.openFrame(new ByteArrayInputStream(bytes));
        assertNotNull(reader);
        assertEquals(streamId, reader.getStreamId());
        assertEquals(sequence, reader.getSequence());
        assertEquals("x", new String(readAll(reader), StandardCharsets.UTF_8));
    }

    @Test
    void escapesPayload() throws IOException {
        byte[] payload = {0x00, (byte) 0xAA, (byte) 0xBB, (byte) 0xFF,
                (byte) 0xEE, (byte) 0xCD, 0x41};
        SocketFrame frame = SocketFrame.data(1, 0, payload);
        byte[] bytes = frame.getBytes();

        assertEquals(12, readUnsignedInt(bytes, 22));
        assertArrayEquals(payload, new SocketFrame(bytes).getPayload());

        SocketFrame parsed = SocketFrame.readFrom(new ByteArrayInputStream(bytes));
        assertNotNull(parsed);
        assertArrayEquals(payload, parsed.getPayload());

        SocketFrame empty = SocketFrame.data(1, 0, new byte[0]);
        assertEquals(SocketFrame.FIXED_LENGTH, empty.getBytes().length);
    }

    @Test
    void readsPayloadIncrementally() throws IOException {
        byte[] payload = new byte[20000];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) i;
        }
        SocketFrame frame = SocketFrame.data(1, 0, payload);
        byte[] wire = frame.getBytes();
        CountingInputStream counting = new CountingInputStream(new ByteArrayInputStream(wire));

        SocketFrame.FrameReader reader = SocketFrame.openFrame(counting);
        assertNotNull(reader);
        assertEquals(SocketFrame.PREFIX_LENGTH, counting.getCount());

        byte[] first = new byte[1];
        assertEquals(1, reader.read(first, 0, 1));
        assertTrue(counting.getCount() < wire.length, "frame must not be fully buffered");
        assertTrue(counting.getCount() > SocketFrame.PREFIX_LENGTH, "payload read should start");

        ByteArrayOutputStream collected = new ByteArrayOutputStream();
        collected.write(first, 0, 1);
        byte[] buffer = new byte[333];
        int read;
        while ((read = reader.read(buffer, 0, buffer.length)) != -1) {
            collected.write(buffer, 0, read);
        }
        assertArrayEquals(payload, collected.toByteArray());
        assertTrue(reader.isFinished());
        assertNull(SocketFrame.openFrame(counting));
    }

    @Test
    void handlesEscapeAcrossBufferBoundary() throws IOException {
        byte[] payload = new byte[8193];
        Arrays.fill(payload, 0, 8191, (byte) 'A');
        payload[8191] = (byte) 0xAA;
        payload[8192] = (byte) 'B';

        SocketFrame frame = SocketFrame.data(1, 0, payload);
        SocketFrame.FrameReader reader = SocketFrame.openFrame(
                new ByteArrayInputStream(frame.getBytes()));
        assertNotNull(reader);

        ByteArrayOutputStream collected = new ByteArrayOutputStream();
        byte[] buffer = new byte[777];
        int read;
        while ((read = reader.read(buffer, 0, buffer.length)) != -1) {
            collected.write(buffer, 0, read);
        }
        assertArrayEquals(payload, collected.toByteArray());
    }

    @Test
    void closeDrainsRemainingFrame() throws IOException {
        SocketFrame first = SocketFrame.data(1, 0, "first");
        SocketFrame second = SocketFrame.data(1, 1, "second");
        ByteArrayOutputStream wire = new ByteArrayOutputStream();
        wire.write(first.getBytes());
        wire.write(second.getBytes());

        InputStream in = new ByteArrayInputStream(wire.toByteArray());
        SocketFrame.FrameReader reader = SocketFrame.openFrame(in);
        assertNotNull(reader);
        assertEquals('f', reader.read());
        reader.close();
        assertTrue(reader.isFinished());

        SocketFrame.FrameReader next = SocketFrame.openFrame(in);
        assertNotNull(next);
        assertEquals("second", new String(readAll(next), StandardCharsets.UTF_8));
    }

    @Test
    void rejectsInvalidFrames() {
        byte[] valid = SocketFrame.data(1, 0, "abcd").getBytes();

        byte[] badVersion = valid.clone();
        badVersion[3] = 0x02;
        expectFrameRejected(badVersion);

        byte[] badType = valid.clone();
        badType[13] = 0x7F;
        expectFrameRejected(badType);

        byte[] badOffset = valid.clone();
        badOffset[12] = 0x7F;
        expectFrameRejected(badOffset);

        byte[] badMonth = valid.clone();
        badMonth[7] = 13;
        expectFrameRejected(badMonth);

        byte[] badTail = valid.clone();
        badTail[badTail.length - 1] = 0x00;
        expectFrameRejected(badTail);

        byte[] lengthMismatch = valid.clone();
        lengthMismatch[25] += 1;
        expectFrameRejected(lengthMismatch);

        byte[] hugeLength = valid.clone();
        Arrays.fill(hugeLength, 22, 26, (byte) 0xFF);
        expectFrameRejected(hugeLength);

        byte[] endWithPayload = SocketFrame.data(1, 0, "x").getBytes();
        endWithPayload[13] = SocketFrame.TYPE_END;
        expectFrameRejected(endWithPayload);

        byte[] badEscape = SocketFrame.data(1, 0, new byte[]{0x00, 0x00}).getBytes();
        badEscape[SocketFrame.PREFIX_LENGTH] = (byte) 0xCD;
        badEscape[SocketFrame.PREFIX_LENGTH + 1] = 0x00;
        expectFrameRejected(badEscape);

        byte[] rawSpecial = SocketFrame.data(1, 0, new byte[]{0x00, 0x00}).getBytes();
        rawSpecial[SocketFrame.PREFIX_LENGTH] = (byte) 0xAA;
        expectFrameRejected(rawSpecial);
    }

    @Test
    void rejectsTruncatedFrames() throws IOException {
        byte[] valid = SocketFrame.data(1, 0, "abcdef").getBytes();
        assertThrows(EOFException.class,
                () -> SocketFrame.readFrom(new ByteArrayInputStream(
                        Arrays.copyOf(valid, valid.length - 3))));

        SocketFrame.FrameReader reader = SocketFrame.openFrame(
                new ByteArrayInputStream(Arrays.copyOf(valid, valid.length - 3)));
        assertNotNull(reader);
        assertThrows(EOFException.class, () -> readAll(reader));
    }

    @Test
    void wrapsSequenceNumbers() {
        assertEquals(1, SocketFrame.nextSequence(0));
        assertEquals(0xFFFFFFFFL, SocketFrame.nextSequence(0xFFFFFFFEL));
        assertEquals(0, SocketFrame.nextSequence(0xFFFFFFFFL));

        assertThrows(IllegalArgumentException.class, () -> SocketFrame.data(-1, 0, "x"));
        assertThrows(IllegalArgumentException.class,
                () -> SocketFrame.data(0x100000000L, 0, "x"));
        assertThrows(IllegalArgumentException.class, () -> SocketFrame.data(1, -1, "x"));
        assertThrows(IllegalArgumentException.class,
                () -> SocketFrame.nextSequence(0x100000000L));
    }

    @Test
    void encodesTimeZoneOffsets() {
        SocketFrame frame = SocketFrame.data(1, 0, "x");
        byte[] bytes = frame.getBytes();
        int code = frame.getTimeZoneOffset().getTotalSeconds() / 900;
        assertEquals((byte) code, bytes[12]);

        bytes[12] = 0x20;
        assertEquals(ZoneOffset.ofHours(8), new SocketFrame(bytes).getTimeZoneOffset());
        bytes[12] = (byte) 0xE0;
        assertEquals(ZoneOffset.ofHours(-8), new SocketFrame(bytes).getTimeZoneOffset());
        bytes[12] = 0x16;
        assertEquals(ZoneOffset.ofHoursMinutes(5, 30), new SocketFrame(bytes).getTimeZoneOffset());
        bytes[12] = 0x17;
        assertEquals(ZoneOffset.ofHoursMinutes(5, 45), new SocketFrame(bytes).getTimeZoneOffset());
    }

    private static void expectFrameRejected(byte[] frame) {
        assertThrows(IllegalArgumentException.class, () -> new SocketFrame(frame));
    }

    private static byte[] readAll(SocketFrame.FrameReader reader) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buffer = new byte[4096];
        int read;
        while ((read = reader.read(buffer, 0, buffer.length)) != -1) {
            out.write(buffer, 0, read);
        }
        return out.toByteArray();
    }

    private static long readUnsignedInt(byte[] data, int offset) {
        return ((long) (data[offset] & 0xFF) << 24)
                | ((long) (data[offset + 1] & 0xFF) << 16)
                | ((long) (data[offset + 2] & 0xFF) << 8)
                | (data[offset + 3] & 0xFF);
    }

    private static boolean containsPair(byte[] data, int first, int second) {
        for (int i = 0; i + 1 < data.length; i++) {
            if ((data[i] & 0xFF) == first && (data[i + 1] & 0xFF) == second) {
                return true;
            }
        }
        return false;
    }

    private static final class CountingInputStream extends FilterInputStream {
        private long count;

        private CountingInputStream(InputStream in) {
            super(in);
        }

        long getCount() {
            return count;
        }

        @Override
        public int read() throws IOException {
            int value = super.read();
            if (value != -1) {
                count++;
            }
            return value;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int read = super.read(b, off, len);
            if (read > 0) {
                count += read;
            }
            return read;
        }
    }
}
