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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@Timeout(15)
class ProtocolConnectionTest {

    @Test
    void requestReceivesStreamingResponse() throws Exception {
        AtomicReference<Throwable> error = new AtomicReference<>();
        AtomicReference<String> received = new AtomicReference<>();

        try (ServerSocket server = new ServerSocket(0)) {
            server.setSoTimeout(5000);
            Thread thread = startServer(server, connection -> {
                ProtocolConnection.Request request = connection.receiveRequest();
                assertNotNull(request, "request");
                received.set(request.getText());

                try (ProtocolConnection.Reply reply = request.reply()) {
                    reply.write("hello ");
                    reply.write("world");
                }
            }, error);

            try (ProtocolConnection connection = connect(server.getLocalPort());
                 ProtocolConnection.Response response = connection.request("你好")) {
                assertEquals("hello world", readText(response));
                assertEquals(1, response.getStreamId());
            }

            joinServer(thread, error);
            assertEquals("你好", received.get());
        }
    }

    @Test
    void connectionSupportsSequentialRequests() throws Exception {
        AtomicReference<Throwable> error = new AtomicReference<>();
        List<String> received = new ArrayList<>();

        try (ServerSocket server = new ServerSocket(0)) {
            server.setSoTimeout(5000);
            Thread thread = startServer(server, connection -> {
                for (int i = 0; i < 2; i++) {
                    ProtocolConnection.Request request = connection.receiveRequest();
                    assertNotNull(request, "request " + i);
                    synchronized (received) {
                        received.add(request.getText());
                    }
                    try (ProtocolConnection.Reply reply = request.reply()) {
                        reply.write("echo:" + request.getText());
                    }
                }
                assertNull(connection.receiveRequest(), "clean close");
            }, error);

            try (ProtocolConnection connection = connect(server.getLocalPort())) {
                try (ProtocolConnection.Response response = connection.request("one")) {
                    assertEquals("echo:one", readText(response));
                }
                try (ProtocolConnection.Response response = connection.request("two")) {
                    assertEquals("echo:two", readText(response));
                }
            }

            joinServer(thread, error);
            assertEquals(Arrays.asList("one", "two"), received);
        }
    }

    @Test
    void largeBinaryReplyIsChunked() throws Exception {
        AtomicReference<Throwable> error = new AtomicReference<>();
        byte[] large = new byte[200000];
        for (int i = 0; i < large.length; i++) {
            large[i] = (byte) (i % 256);
        }

        try (ServerSocket server = new ServerSocket(0)) {
            server.setSoTimeout(5000);
            Thread thread = startServer(server, connection -> {
                ProtocolConnection.Request request = connection.receiveRequest();
                assertNotNull(request, "request");
                try (ProtocolConnection.Reply reply = request.reply()) {
                    reply.write(large);
                }
            }, error);

            try (ProtocolConnection connection = connect(server.getLocalPort());
                 ProtocolConnection.Response response = connection.request("large")) {
                assertArrayEquals(large, readAll(response));
            }

            joinServer(thread, error);
        }
    }

    @Test
    void propagatesServerError() throws Exception {
        AtomicReference<Throwable> error = new AtomicReference<>();

        try (ServerSocket server = new ServerSocket(0)) {
            server.setSoTimeout(5000);
            Thread thread = startServer(server, connection -> {
                ProtocolConnection.Request request = connection.receiveRequest();
                assertNotNull(request, "request");
                try (ProtocolConnection.Reply reply = request.reply()) {
                    reply.write("partial");
                    reply.error(ErrorCode.SERVER_BUSY, "boom");
                }
            }, error);

            try (ProtocolConnection connection = connect(server.getLocalPort());
                 ProtocolConnection.Response response = connection.request("error")) {
                ByteArrayOutputStream received = new ByteArrayOutputStream();
                ProtocolException exception = assertThrows(ProtocolException.class,
                        () -> response.copyTo(received));
                assertEquals(ErrorCode.SERVER_BUSY, exception.getErrorCode());
                assertEquals("SERVER_BUSY", exception.getErrorName());
                assertEquals("boom", exception.getServerMessage());
                assertTrue(exception.getMessage().contains("boom"));
                assertEquals("partial",
                        new String(received.toByteArray(), StandardCharsets.UTF_8));
            }

            joinServer(thread, error);
        }
    }

    @Test
    void earlyResponseCloseKeepsConnectionUsable() throws Exception {
        AtomicReference<Throwable> error = new AtomicReference<>();

        try (ServerSocket server = new ServerSocket(0)) {
            server.setSoTimeout(5000);
            Thread thread = startServer(server, connection -> {
                ProtocolConnection.Request first = connection.receiveRequest();
                assertNotNull(first, "first request");
                try (ProtocolConnection.Reply reply = first.reply()) {
                    reply.write("first");
                    reply.write("second");
                    reply.write("third");
                }

                ProtocolConnection.Request second = connection.receiveRequest();
                assertNotNull(second, "second request");
                try (ProtocolConnection.Reply reply = second.reply()) {
                    reply.write("after-close");
                }
            }, error);

            try (ProtocolConnection connection = connect(server.getLocalPort())) {
                try (ProtocolConnection.Response response = connection.request("one")) {
                    assertEquals('f', response.read());
                    // close() 丢弃剩余响应，连接回到帧边界
                }
                try (ProtocolConnection.Response response = connection.request("two")) {
                    assertEquals("after-close", readText(response));
                }
            }

            joinServer(thread, error);
        }
    }

    @Test
    void rejectsNonRequestFrame() throws Exception {
        AtomicReference<Throwable> error = new AtomicReference<>();

        try (ServerSocket server = new ServerSocket(0)) {
            server.setSoTimeout(5000);
            Thread thread = new Thread(() -> {
                try (Socket socket = server.accept()) {
                    socket.setSoTimeout(5000);
                    try (ProtocolConnection connection = ProtocolConnection.of(socket)) {
                        try {
                            connection.receiveRequest();
                            fail("expected IOException");
                        } catch (IOException expected) {
                            // expected: ERROR frame is sent before the exception
                        }
                    }
                } catch (Throwable e) {
                    error.set(e);
                }
            });
            thread.start();

            try (Socket socket = new Socket("127.0.0.1", server.getLocalPort());
                 BufferedInputStream in = new BufferedInputStream(socket.getInputStream());
                 BufferedOutputStream out = new BufferedOutputStream(socket.getOutputStream())) {
                socket.setSoTimeout(5000);
                out.write(SocketFrame.data(5, 0, "bad").getBytes());
                out.flush();

                SocketFrame.FrameReader frame = SocketFrame.openFrame(in);
                assertNotNull(frame);
                assertEquals(SocketFrame.TYPE_ERROR, frame.getType());
                assertEquals(5, frame.getStreamId());
                String message = new String(readAll(frame), StandardCharsets.UTF_8);
                assertTrue(message.contains("REQUEST"));
            }

            joinServer(thread, error);
        }
    }

    private interface ServerHandler {
        void handle(ProtocolConnection connection) throws Exception;
    }

    private static Thread startServer(ServerSocket server, ServerHandler handler,
                                      AtomicReference<Throwable> error) {
        Thread thread = new Thread(() -> {
            try (Socket socket = server.accept()) {
                socket.setSoTimeout(5000);
                try (ProtocolConnection connection = ProtocolConnection.of(socket)) {
                    handler.handle(connection);
                }
            } catch (Throwable e) {
                error.set(e);
            }
        });
        thread.start();
        return thread;
    }

    private static ProtocolConnection connect(int port) throws IOException {
        Socket socket = new Socket("127.0.0.1", port);
        socket.setSoTimeout(5000);
        try {
            return ProtocolConnection.of(socket);
        } catch (IOException e) {
            socket.close();
            throw e;
        }
    }

    private static void joinServer(Thread thread, AtomicReference<Throwable> error) throws Exception {
        thread.join(5000);
        assertFalse(thread.isAlive(), "server thread did not finish");
        if (error.get() != null) {
            throw new AssertionError("server thread failed", error.get());
        }
    }

    private static byte[] readAll(ProtocolConnection.Response response) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        response.copyTo(out);
        return out.toByteArray();
    }

    private static String readText(ProtocolConnection.Response response) throws IOException {
        return new String(readAll(response), StandardCharsets.UTF_8);
    }

    private static byte[] readAll(SocketFrame.FrameReader frame) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buffer = new byte[4096];
        int read;
        while ((read = frame.read(buffer, 0, buffer.length)) != -1) {
            out.write(buffer, 0, read);
        }
        return out.toByteArray();
    }
}
