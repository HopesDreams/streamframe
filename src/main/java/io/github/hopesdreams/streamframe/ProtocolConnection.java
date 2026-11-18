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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

/**
 * 对 {@link SocketFrame} 的高层封装，直接提供“请求 / 回复 / 流式响应”能力。
 *
 * <p>业务代码不需要再手工处理 InputStream、OutputStream、帧头、流 ID、
 * 序号和 END 帧，只需要：</p>
 *
 * <pre>{@code
 * // 客户端
 * try (ProtocolConnection connection = ProtocolConnection.connect("127.0.0.1", 1818);
 *      ProtocolConnection.Response response = connection.request("Hello")) {
 *     response.copyTo(System.out);
 * }
 *
 * // 服务端
 * try (ProtocolConnection connection = ProtocolConnection.of(socket)) {
 *     ProtocolConnection.Request request = connection.receiveRequest();
 *     if (request != null) {
 *         try (ProtocolConnection.Reply reply = request.reply()) {
 *             reply.write("第一段");
 *             reply.write("第二段");
 *         }
 *     }
 * }
 * }</pre>
 *
 * <p>每个 {@code write} 调用会作为一个 DATA 帧发送并立即 flush；{@link Reply#close()}
 * 会自动发送 END 帧。{@link Response} 本身是字节输入流，可以在整个响应
 * 结束之前逐块读取和输出。</p>
 *
 * <p>同一条连接上的请求/响应按顺序使用：读取或关闭上一个 {@link Response}
 * 后，再发起下一个请求。</p>
 *
 * <p>ERROR 帧携带 2 字节错误码和 UTF-8 错误信息；{@link Response} 收到
 * ERROR 时抛出 {@link ProtocolException}。</p>
 */
public final class ProtocolConnection implements Closeable {
    /** 单个 DATA 帧的最大原始负载，避免转义后超过 1 MiB 的帧上限。 */
    private static final int REPLY_CHUNK_SIZE = 64 * 1024;

    private final Socket socket;
    private final BufferedInputStream in;
    private final BufferedOutputStream out;

    private long nextStreamId = 1;
    private boolean closed;

    private ProtocolConnection(Socket socket) throws IOException {
        this.socket = Objects.requireNonNull(socket, "socket");
        socket.setTcpNoDelay(true);
        this.in = new BufferedInputStream(socket.getInputStream());
        this.out = new BufferedOutputStream(socket.getOutputStream());
    }

    /**
     * 连接服务端。
     */
    public static ProtocolConnection connect(String host, int port) throws IOException {
        Socket socket = new Socket(host, port);
        try {
            return new ProtocolConnection(socket);
        } catch (IOException e) {
            closeQuietly(socket);
            throw e;
        }
    }

    /**
     * 包装一个已经建立的 Socket，常用于服务端 accept 之后。
     * 返回的连接关闭时会同时关闭该 Socket。
     */
    public static ProtocolConnection of(Socket socket) throws IOException {
        try {
            return new ProtocolConnection(socket);
        } catch (IOException e) {
            closeQuietly(socket);
            throw e;
        }
    }

    public Socket getSocket() {
        return socket;
    }

    public SocketAddress getLocalSocketAddress() {
        return socket.getLocalSocketAddress();
    }

    public SocketAddress getRemoteSocketAddress() {
        return socket.getRemoteSocketAddress();
    }

    public boolean isClosed() {
        return closed;
    }

    /**
     * 发送请求并返回可流式读取的响应。
     *
     * @param text UTF-8 请求文本
     */
    public Response request(String text) throws IOException {
        Objects.requireNonNull(text, "text");
        return request(text.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * 发送请求并返回可流式读取的响应。
     */
    public Response request(byte[] payload) throws IOException {
        Objects.requireNonNull(payload, "payload");
        long streamId = allocateStreamId();

        SocketFrame frame;
        try {
            frame = SocketFrame.request(streamId, 0, payload);
        } catch (IllegalArgumentException e) {
            throw new IOException("请求数据非法：" + e.getMessage(), e);
        }
        send(frame);
        return new Response(this, streamId);
    }

    /**
     * 服务端读取一个完整请求；流正常结束时返回 {@code null}。
     */
    public Request receiveRequest() throws IOException {
        SocketFrame.FrameReader frame = readFrame();
        if (frame == null) {
            return null;
        }

        if (frame.getType() != SocketFrame.TYPE_REQUEST) {
            throw reject(frame, ErrorCode.INVALID_REQUEST,
                    "期望 REQUEST 帧，收到类型 0x" + String.format("%02X", frame.getType()));
        }
        if (frame.getSequence() != 0) {
            throw reject(frame, ErrorCode.PROTOCOL_ERROR, "REQUEST 帧序号必须为 0");
        }

        byte[] payload;
        try {
            payload = readAll(frame);
        } catch (IllegalArgumentException e) {
            throw new IOException("协议数据非法：" + e.getMessage(), e);
        }
        return new Request(this, frame.getStreamId(), payload);
    }

    /**
     * 发送一个已经构造好的帧，供特殊场景使用。
     */
    public void send(SocketFrame frame) throws IOException {
        Objects.requireNonNull(frame, "frame");
        if (closed) {
            throw new IOException("连接已关闭");
        }
        out.write(frame.getBytes());
        out.flush();
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        socket.close();
    }

    private long allocateStreamId() throws IOException {
        if (closed) {
            throw new IOException("连接已关闭");
        }
        long streamId = nextStreamId;
        nextStreamId = nextStreamId >= 0xFFFFFFFFL ? 1 : nextStreamId + 1;
        return streamId;
    }

    private SocketFrame.FrameReader readFrame() throws IOException {
        try {
            return SocketFrame.openFrame(in);
        } catch (IllegalArgumentException e) {
            throw new IOException("协议数据非法：" + e.getMessage(), e);
        }
    }

    /**
     * 丢弃非法帧并回一个 ERROR 帧，然后把异常交给调用方。
     */
    private IOException reject(SocketFrame.FrameReader frame, int errorCode,
                               String message) throws IOException {
        long streamId = frame.getStreamId();
        try {
            frame.close();
        } catch (IllegalArgumentException e) {
            throw new IOException("协议数据非法：" + e.getMessage(), e);
        }
        send(SocketFrame.error(streamId, 0, errorCode, message));
        return new IOException(message);
    }

    private static byte[] readAll(SocketFrame.FrameReader frame) throws IOException {
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        byte[] buffer = new byte[4096];
        int read;
        while ((read = frame.read(buffer, 0, buffer.length)) != -1) {
            payload.write(buffer, 0, read);
        }
        return payload.toByteArray();
    }

    private static void closeQuietly(Socket socket) {
        try {
            socket.close();
        } catch (IOException ignored) {
            // 忽略关闭失败
        }
    }

    /**
     * 服务端收到的一次完整请求。
     */
    public static final class Request {
        private final ProtocolConnection connection;
        private final long streamId;
        private final byte[] payload;
        private boolean replied;

        private Request(ProtocolConnection connection, long streamId, byte[] payload) {
            this.connection = connection;
            this.streamId = streamId;
            this.payload = payload;
        }

        public long getStreamId() {
            return streamId;
        }

        public byte[] getPayload() {
            return payload.clone();
        }

        public String getText() {
            return new String(payload, StandardCharsets.UTF_8);
        }

        /**
         * 开始向该请求回复。一个请求只能调用一次。
         */
        public Reply reply() throws IOException {
            if (replied) {
                throw new IOException("该请求已经回复过");
            }
            replied = true;
            return new Reply(connection, streamId);
        }
    }

    /**
     * 服务端回复流。每次 write 发送一个 DATA 帧，close 发送 END 帧。
     */
    public static final class Reply implements Closeable {
        private final ProtocolConnection connection;
        private final long streamId;
        private long sequence;
        private boolean closed;

        private Reply(ProtocolConnection connection, long streamId) {
            this.connection = connection;
            this.streamId = streamId;
        }

        public long getStreamId() {
            return streamId;
        }

        public boolean isClosed() {
            return closed;
        }

        /**
         * 发送一段 UTF-8 文本。文本过大时会自动拆成多个 DATA 帧。
         */
        public void write(String text) throws IOException {
            Objects.requireNonNull(text, "text");
            write(text.getBytes(StandardCharsets.UTF_8));
        }

        /**
         * 发送一段字节数据。数据过大时会自动拆成多个 DATA 帧。
         */
        public void write(byte[] data) throws IOException {
            Objects.requireNonNull(data, "data");
            ensureOpen();
            if (data.length == 0) {
                return;
            }

            for (int offset = 0; offset < data.length; offset += REPLY_CHUNK_SIZE) {
                int length = Math.min(REPLY_CHUNK_SIZE, data.length - offset);
                byte[] chunk = (offset == 0 && length == data.length)
                        ? data
                        : Arrays.copyOfRange(data, offset, offset + length);
                connection.send(SocketFrame.data(streamId, sequence, chunk));
                sequence = SocketFrame.nextSequence(sequence);
            }
        }

        /**
         * 结束回复，发送 END 帧。
         */
        public void end() throws IOException {
            if (closed) {
                return;
            }
            connection.send(SocketFrame.end(streamId, sequence));
            sequence = SocketFrame.nextSequence(sequence);
            closed = true;
        }

        /**
         * 发送 ERROR 帧并结束回复，使用 {@link ErrorCode#UNSPECIFIED} 错误码。
         */
        public void error(String message) throws IOException {
            error(ErrorCode.UNSPECIFIED, message);
        }

        /**
         * 发送带标准错误码的 ERROR 帧并结束回复。
         */
        public void error(int errorCode, String message) throws IOException {
            Objects.requireNonNull(message, "message");
            ensureOpen();
            connection.send(SocketFrame.error(streamId, sequence, errorCode, message));
            closed = true;
        }

        @Override
        public void close() throws IOException {
            end();
        }

        private void ensureOpen() throws IOException {
            if (closed) {
                throw new IOException("回复流已关闭");
            }
        }
    }

    /**
     * 客户端响应流：跨多个 DATA 帧提供连续的负载字节流。
     *
     * <p>读到 -1 表示收到 END 帧；收到 ERROR 帧时抛出带服务端错误文本的
     * {@link IOException}。</p>
     */
    public static final class Response extends InputStream {
        private final ProtocolConnection connection;
        private final long streamId;
        private final byte[] single = new byte[1];

        private SocketFrame.FrameReader current;
        private long expectedSequence;
        private boolean finished;

        private Response(ProtocolConnection connection, long streamId) {
            this.connection = connection;
            this.streamId = streamId;
        }

        public long getStreamId() {
            return streamId;
        }

        @Override
        public int read() throws IOException {
            int read = read(single, 0, 1);
            return read == -1 ? -1 : single[0] & 0xFF;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            Objects.requireNonNull(b, "b");
            if (off < 0 || len < 0 || len > b.length - off) {
                throw new IndexOutOfBoundsException();
            }
            if (len == 0) {
                return 0;
            }

            while (!finished) {
                if (current == null) {
                    SocketFrame.FrameReader frame;
                    try {
                        frame = connection.readFrame();
                    } catch (IOException e) {
                        finished = true;
                        throw e;
                    }
                    if (frame == null) {
                        finished = true;
                        throw new EOFException("服务端在 END 帧之前关闭连接");
                    }
                    if (frame.getStreamId() != streamId) {
                        finished = true;
                        throw new IOException("响应流 ID 不匹配：期望 " + streamId
                                + "，实际 " + frame.getStreamId());
                    }
                    if (frame.getSequence() != expectedSequence) {
                        finished = true;
                        throw new IOException("响应序号不匹配：期望 " + expectedSequence
                                + "，实际 " + frame.getSequence());
                    }
                    expectedSequence = SocketFrame.nextSequence(expectedSequence);

                    switch (frame.getType()) {
                        case SocketFrame.TYPE_DATA:
                            current = frame;
                            break;
                        case SocketFrame.TYPE_END:
                            finished = true;
                            closeFrame(frame);
                            return -1;
                        case SocketFrame.TYPE_ERROR:
                            byte[] errorPayload;
                            try {
                                errorPayload = ProtocolConnection.readAll(frame);
                            } catch (IOException e) {
                                finished = true;
                                throw e;
                            } catch (IllegalArgumentException e) {
                                finished = true;
                                throw new IOException("协议数据非法：" + e.getMessage(), e);
                            }
                            if (errorPayload.length < 2) {
                                finished = true;
                                throw new IOException("ERROR 帧负载长度不足，缺少错误码");
                            }
                            int errorCode = ((errorPayload[0] & 0xFF) << 8)
                                    | (errorPayload[1] & 0xFF);
                            String errorMessage = new String(errorPayload, 2,
                                    errorPayload.length - 2, StandardCharsets.UTF_8);
                            finished = true;
                            throw new ProtocolException(errorCode, errorMessage);
                        default:
                            finished = true;
                            closeFrame(frame);
                            throw new IOException("未知帧类型：0x"
                                    + String.format("%02X", frame.getType()));
                    }
                }

                int read;
                try {
                    read = current.read(b, off, len);
                } catch (IllegalArgumentException e) {
                    finished = true;
                    throw new IOException("协议数据非法：" + e.getMessage(), e);
                }
                if (read != -1) {
                    return read;
                }
                current = null;
            }
            return -1;
        }

        @Override
        public int available() {
            return current == null ? 0 : current.available();
        }

        private void closeFrame(SocketFrame.FrameReader frame) throws IOException {
            try {
                frame.close();
            } catch (IllegalArgumentException e) {
                throw new IOException("协议数据非法：" + e.getMessage(), e);
            }
        }

        /**
         * 关闭响应。如果响应还没有读完，会读取并丢弃剩余数据，
         * 以便连接可以继续用于下一个请求。
         */
        @Override
        public void close() throws IOException {
            if (finished) {
                return;
            }
            byte[] discard = new byte[4096];
            while (read(discard, 0, discard.length) != -1) {
                // 丢弃剩余响应
            }
        }

        /**
         * 把响应剩余内容复制到输出流，边收边写并立即 flush。
         *
         * @return 复制的字节数
         */
        public long copyTo(OutputStream output) throws IOException {
            Objects.requireNonNull(output, "output");
            byte[] buffer = new byte[8192];
            long total = 0;
            int read;
            while ((read = read(buffer, 0, buffer.length)) != -1) {
                output.write(buffer, 0, read);
                output.flush();
                total += read;
            }
            output.flush();
            return total;
        }
    }
}
