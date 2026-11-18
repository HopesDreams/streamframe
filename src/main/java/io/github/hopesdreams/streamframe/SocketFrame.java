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

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Objects;

/**
 * 一个完整的通信数据帧，负责协议数据的封装、解析和帧内流式读取。
 *
 * <pre>
 * | 字段     | 值        | 长度     |
 * | 通信头   | 0xAA 0xBB | 2 字节   |
 * | 版本号   | 0x00 0x04 | 2 字节   |
 * | 压缩算法 | 0xnn      | 1 字节   |
 * | 时间戳   | /         | 8 字节   |
 * | 帧类型   | 0xnn      | 1 字节   |
 * | 流 ID    | /         | 4 字节   |
 * | 序号     | /         | 4 字节   |
 * | 数据长度 | /         | 4 字节   |
 * | 段落内容 | /         | 变长     |
 * | 通信尾   | 0xFF 0xEE | 2 字节   |
 * </pre>
 *
 * <p>时间戳固定 8 字节，依次为：年（2 字节，大端）、月、日、时、分、秒、
 * 时区。时区字节是 UTC 偏移量（单位 15 分钟）的 8 位有符号补码：
 * 0x00 表示 UTC，0x04 表示 UTC+1，0x20 表示 UTC+8，0xE0 表示 UTC-8，
 * 0x16 表示 UTC+5:30，0x17 表示 UTC+5:45。</p>
 *
 * <p>帧类型：0x01 DATA（数据块）、0x02 END（流结束）、0x03 ERROR（错误）、
 * 0x10 REQUEST（请求）。客户端用 REQUEST 发起请求，服务端用 DATA
 * 连续发送数据块，最后用 END 结束流。流 ID 和序号都是 4 字节无符号
 * 大端整数；同一流同一方向内，序号从 0 开始逐帧加 1，到达
 * 0xFFFFFFFF 后回绕。</p>
 *
 * <p>ERROR 帧的负载格式为：错误码（2 字节无符号大端）+ UTF-8 错误信息。
 * 标准错误码定义见 {@link ErrorCode}。</p>
 *
 * <p>数据长度是转义后的段落内容字节数，接收方按该长度精确读取，
 * 因此流 ID、序号和负载中出现 0xFF 0xEE 也不会被误判为通信尾。
 * 段落内容中的 0xAA、0xBB、0xCD、0xFF、0xEE 在编码时仍会在前面
 * 添加转义字节 0xCD：例如 0xFF 编码为 0xCD 0xFF，0xCD 本身编码为
 * 0xCD 0xCD；解码时再还原。通信头、版本号、压缩算法、时间戳、
 * 帧类型、流 ID、序号、数据长度和通信尾本身不参与转义。</p>
 *
 * <p>需要“边收边处理”时使用 {@link #openFrame(InputStream)}：
 * 返回的 {@link FrameReader} 是一个负载字节流，读取过程中才从底层
 * 流中取数据并反转义；读到 -1 时通信尾已经校验并消费，可以继续
 * 读取下一帧。{@link #readFrom(InputStream)} 仍保留，用于一次性
 * 读取完整帧的简单场景。</p>
 */
public final class SocketFrame {
    /** 通信头：2 字节。 */
    public static final int HEADER = 0xAABB;
    public static final int HEADER_HIGH = 0xAA;
    public static final int HEADER_LOW = 0xBB;

    /** 通信尾：2 字节。 */
    public static final int TAIL = 0xFFEE;
    public static final int TAIL_HIGH = 0xFF;
    public static final int TAIL_LOW = 0xEE;

    /** 转义字节：段落内容中的特殊字节前都会添加该字节。 */
    public static final int ESCAPE = 0xCD;

    /** 当前协议版本。ERROR 帧加入标准错误码后升级为 0x0004。 */
    public static final int VERSION = 0x0004;

    /** 不压缩。 */
    public static final int COMPRESSION_NONE = 0x00;

    /** 数据块帧。 */
    public static final int TYPE_DATA = 0x01;
    /** 流结束帧，负载必须为空。 */
    public static final int TYPE_END = 0x02;
    /** 错误帧，负载为错误文本。 */
    public static final int TYPE_ERROR = 0x03;
    /** 请求帧，一次完整的请求。 */
    public static final int TYPE_REQUEST = 0x10;

    /** 数据长度字段之前的固定前缀长度：2 + 2 + 1 + 8 + 1 + 4 + 4 + 4。 */
    public static final int PREFIX_LENGTH = 26;

    /** 空负载帧的总长度：固定前缀 26 字节 + 通信尾 2 字节。 */
    public static final int FIXED_LENGTH = 28;

    /** 编码（含转义）后的单帧最大长度，防止异常数据耗尽内存（1 MiB）。 */
    public static final int MAX_FRAME_LENGTH = 1024 * 1024;

    private final int version;
    private final LocalDateTime timestamp;
    private final ZoneOffset timeZoneOffset;
    private final int compressionAlgorithm;
    private final int type;
    private final long streamId;
    private final long sequence;
    private final byte[] payload;
    private final byte[] bytes;

    /**
     * 解析收到的完整数据帧。
     *
     * @param frame 完整数据帧
     * @throws IllegalArgumentException 数据帧为 {@code null}、长度不足或不匹配、
     *                                  通信头/尾错误、版本或压缩算法或帧类型不受支持、
     *                                  时间戳/时区非法、END 帧带负载
     */
    public SocketFrame(byte[] frame) {
        if (frame == null) {
            throw new IllegalArgumentException("数据帧不能为 null");
        }
        if (frame.length < FIXED_LENGTH) {
            throw new IllegalArgumentException("数据帧长度不足：" + frame.length + " < " + FIXED_LENGTH);
        }
        if (frame.length > MAX_FRAME_LENGTH) {
            throw new IllegalArgumentException("数据帧超过最大长度：" + frame.length + " > " + MAX_FRAME_LENGTH);
        }

        int head = readUnsignedShort(frame, 0);
        if (head != HEADER) {
            throw new IllegalArgumentException(String.format("通信头错误：0x%04X", head));
        }
        int tail = readUnsignedShort(frame, frame.length - 2);
        if (tail != TAIL) {
            throw new IllegalArgumentException(String.format("通信尾错误：0x%04X", tail));
        }

        Header header = parseHeader(frame);
        if (frame.length != FIXED_LENGTH + header.payloadLength) {
            throw new IllegalArgumentException("数据长度与帧长不匹配：帧长 " + frame.length
                    + "，数据长度 " + header.payloadLength
                    + "，期望帧长 " + (FIXED_LENGTH + header.payloadLength));
        }

        this.version = header.version;
        this.compressionAlgorithm = header.compressionAlgorithm;
        this.timestamp = header.timestamp;
        this.timeZoneOffset = header.timeZoneOffset;
        this.type = header.type;
        this.streamId = header.streamId;
        this.sequence = header.sequence;
        this.payload = unescape(Arrays.copyOfRange(frame, PREFIX_LENGTH,
                PREFIX_LENGTH + (int) header.payloadLength));
        validateErrorPayload(this.type, this.payload);
        this.bytes = frame.clone();
    }

    private SocketFrame(int type, long streamId, long sequence, byte[] payload, OffsetDateTime timestamp) {
        OffsetDateTime value = Objects.requireNonNull(timestamp, "timestamp");
        this.version = VERSION;
        this.compressionAlgorithm = COMPRESSION_NONE;
        this.type = checkType(type);
        this.streamId = checkUnsignedInt(streamId, "streamId");
        this.sequence = checkUnsignedInt(sequence, "sequence");
        this.payload = Objects.requireNonNull(payload, "payload").clone();
        if (this.type == TYPE_END && this.payload.length != 0) {
            throw new IllegalArgumentException("END 帧不能携带数据");
        }
        validateErrorPayload(this.type, this.payload);
        if (this.payload.length > MAX_FRAME_LENGTH - FIXED_LENGTH) {
            throw new IllegalArgumentException("段落内容过长：" + this.payload.length
                    + " > " + (MAX_FRAME_LENGTH - FIXED_LENGTH));
        }
        this.timestamp = value.toLocalDateTime();
        this.timeZoneOffset = value.getOffset();
        this.bytes = encode();
    }

    /**
     * 用已经解析出的帧头信息和原始负载构造帧，供 readFrom 使用。
     */
    private SocketFrame(Header header, byte[] payload) {
        this.version = header.version;
        this.compressionAlgorithm = header.compressionAlgorithm;
        this.timestamp = header.timestamp;
        this.timeZoneOffset = header.timeZoneOffset;
        this.type = header.type;
        this.streamId = header.streamId;
        this.sequence = header.sequence;
        this.payload = Objects.requireNonNull(payload, "payload").clone();
        validateErrorPayload(this.type, this.payload);
        this.bytes = encode();
    }

    /**
     * 构造 REQUEST 帧。
     */
    public static SocketFrame request(long streamId, long sequence, byte[] payload) {
        return new SocketFrame(TYPE_REQUEST, streamId, sequence, payload, now());
    }

    /**
     * 构造 REQUEST 帧，负载按 UTF-8 编码。
     */
    public static SocketFrame request(long streamId, long sequence, String text) {
        Objects.requireNonNull(text, "text");
        return request(streamId, sequence, text.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * 构造 DATA 帧。
     */
    public static SocketFrame data(long streamId, long sequence, byte[] payload) {
        return new SocketFrame(TYPE_DATA, streamId, sequence, payload, now());
    }

    /**
     * 构造 DATA 帧，负载按 UTF-8 编码。
     */
    public static SocketFrame data(long streamId, long sequence, String text) {
        Objects.requireNonNull(text, "text");
        return data(streamId, sequence, text.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * 构造 END 帧。
     */
    public static SocketFrame end(long streamId, long sequence) {
        return new SocketFrame(TYPE_END, streamId, sequence, new byte[0], now());
    }

    /**
     * 构造 ERROR 帧，使用 {@link ErrorCode#UNSPECIFIED} 错误码。
     */
    public static SocketFrame error(long streamId, long sequence, String message) {
        return error(streamId, sequence, ErrorCode.UNSPECIFIED, message);
    }

    /**
     * 构造带标准错误码的 ERROR 帧。负载格式为：错误码（2 字节大端）+ UTF-8 错误信息。
     *
     * @param errorCode 0..0xFFFF 的无符号错误码
     * @param message   UTF-8 错误信息
     */
    public static SocketFrame error(long streamId, long sequence, int errorCode, String message) {
        Objects.requireNonNull(message, "message");
        if (errorCode < 0 || errorCode > 0xFFFF) {
            throw new IllegalArgumentException("错误码超出 2 字节无符号范围：" + errorCode);
        }
        byte[] text = message.getBytes(StandardCharsets.UTF_8);
        byte[] payload = new byte[2 + text.length];
        payload[0] = (byte) ((errorCode >>> 8) & 0xFF);
        payload[1] = (byte) (errorCode & 0xFF);
        System.arraycopy(text, 0, payload, 2, text.length);
        return new SocketFrame(TYPE_ERROR, streamId, sequence, payload, now());
    }

    /**
     * 打开一个帧，返回可以逐块读取负载的 {@link FrameReader}。
     *
     * <p>本方法只读取固定前缀，不读取负载内容；负载在
     * {@link FrameReader#read(byte[], int, int)} 时按需从底层流读取。
     * 读到 -1 时当前帧的通信尾已经校验并消费，可以继续调用
     * {@code openFrame} 读取下一帧。流正常结束且没有新帧时返回
     * {@code null}。</p>
     *
     * @param in 输入流，必须位于帧边界
     * @return 帧读取器；流正常结束且没有新帧时返回 {@code null}
     * @throws IOException              读流失败、通信头错误或帧未接收完整
     * @throws IllegalArgumentException  帧头字段非法（版本、时区、类型、长度等）
     */
    public static FrameReader openFrame(InputStream in) throws IOException {
        Objects.requireNonNull(in, "in");

        int first = in.read();
        if (first == -1) {
            return null;
        }
        if (first != HEADER_HIGH) {
            throw new IOException(String.format("通信头错误：0x%02X", first));
        }
        int second = in.read();
        if (second == -1) {
            throw new EOFException("连接在通信头之后关闭");
        }
        if (second != HEADER_LOW) {
            throw new IOException(String.format("通信头错误：0x%02X%02X", first, second));
        }

        byte[] prefix = new byte[PREFIX_LENGTH];
        prefix[0] = (byte) first;
        prefix[1] = (byte) second;
        readFully(in, prefix, 2, PREFIX_LENGTH - 2);

        Header header = parseHeader(prefix);
        return new FrameReader(in, header);
    }

    /**
     * 一次性读取一个完整数据帧，适合请求/响应等小数据场景。
     *
     * <p>内部通过 {@link #openFrame(InputStream)} 逐块读取并收集负载，
     * 因此仍然保留帧内流式读取的校验逻辑，只是最终把负载聚合为
     * 一个 {@link SocketFrame} 对象。</p>
     *
     * @param in 输入流，必须位于帧边界
     * @return 解析后的数据帧；流正常结束且没有新帧时返回 {@code null}
     * @throws IOException              读流失败、通信头错误或帧未接收完整
     * @throws IllegalArgumentException  帧结构非法
     */
    public static SocketFrame readFrom(InputStream in) throws IOException {
        FrameReader frame = openFrame(in);
        if (frame == null) {
            return null;
        }

        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        byte[] buffer = new byte[4096];
        int read;
        while ((read = frame.read(buffer, 0, buffer.length)) != -1) {
            payload.write(buffer, 0, read);
        }
        return new SocketFrame(frame.header, payload.toByteArray());
    }

    /**
     * 返回下一个序号，到达 0xFFFFFFFF 后回绕到 0。
     */
    public static long nextSequence(long sequence) {
        checkUnsignedInt(sequence, "sequence");
        return (sequence + 1) & 0xFFFFFFFFL;
    }

    private byte[] encode() {
        byte[] escapedPayload = escape(payload);
        int frameLength = FIXED_LENGTH + escapedPayload.length;
        if (frameLength > MAX_FRAME_LENGTH) {
            throw new IllegalArgumentException("转义后的数据帧超过最大长度：" + frameLength
                    + " > " + MAX_FRAME_LENGTH);
        }

        ByteArrayOutputStream out = new ByteArrayOutputStream(frameLength);
        out.write(HEADER_HIGH);
        out.write(HEADER_LOW);

        out.write((version >>> 8) & 0xFF);
        out.write(version & 0xFF);

        out.write(compressionAlgorithm & 0xFF);

        out.write((timestamp.getYear() >>> 8) & 0xFF);
        out.write(timestamp.getYear() & 0xFF);
        out.write(timestamp.getMonthValue());
        out.write(timestamp.getDayOfMonth());
        out.write(timestamp.getHour());
        out.write(timestamp.getMinute());
        out.write(timestamp.getSecond());
        out.write(encodeOffset(timeZoneOffset));

        out.write(type);
        writeUnsignedInt(out, streamId);
        writeUnsignedInt(out, sequence);
        writeUnsignedInt(out, escapedPayload.length);

        out.write(escapedPayload, 0, escapedPayload.length);

        out.write(TAIL_HIGH);
        out.write(TAIL_LOW);
        return out.toByteArray();
    }

    /**
     * 编码时区字节：UTC 偏移量以 15 分钟为单位，按 8 位有符号补码存储。
     */
    private static int encodeOffset(ZoneOffset offset) {
        int totalSeconds = offset.getTotalSeconds();
        if (totalSeconds % 900 != 0) {
            throw new IllegalArgumentException("时区偏移必须是 15 分钟的整数倍：" + offset);
        }
        return (totalSeconds / 900) & 0xFF;
    }

    /**
     * 解码时区字节。15 分钟单位乘以 15 得到分钟数。
     */
    private static ZoneOffset decodeOffset(int offsetCode) {
        try {
            return ZoneOffset.ofTotalSeconds(offsetCode * 900);
        } catch (DateTimeException e) {
            throw new IllegalArgumentException(String.format(
                    "时区偏移非法：0x%02X（%d 分钟）", offsetCode & 0xFF, offsetCode * 15), e);
        }
    }

    /**
     * 对段落内容做转义：特殊字节前添加 0xCD。
     */
    private static byte[] escape(byte[] payload) {
        ByteArrayOutputStream out = new ByteArrayOutputStream(payload.length);
        for (byte b : payload) {
            int value = b & 0xFF;
            if (needsEscape(value)) {
                out.write(ESCAPE);
            }
            out.write(value);
        }
        return out.toByteArray();
    }

    /**
     * 还原被转义的段落内容，并校验转义序列完整、合法。
     */
    private static byte[] unescape(byte[] wirePayload) {
        ByteArrayOutputStream out = new ByteArrayOutputStream(wirePayload.length);
        for (int i = 0; i < wirePayload.length; i++) {
            int value = wirePayload[i] & 0xFF;
            if (value == ESCAPE) {
                if (i + 1 >= wirePayload.length) {
                    throw new IllegalArgumentException("转义字节 0xCD 后缺少被转义的数据（位置 " + i + "）");
                }
                int escaped = wirePayload[++i] & 0xFF;
                if (!needsEscape(escaped)) {
                    throw new IllegalArgumentException(String.format(
                            "非法转义序列 0xCD 0x%02X（位置 %d）", escaped, i - 1));
                }
                out.write(escaped);
            } else if (needsEscape(value)) {
                throw new IllegalArgumentException(String.format(
                        "特殊字节 0x%02X 未转义（位置 %d）", value, i));
            } else {
                out.write(value);
            }
        }
        return out.toByteArray();
    }

    /**
     * 需要转义的字节：0xAA、0xBB、0xCD、0xFF、0xEE。
     */
    private static boolean needsEscape(int value) {
        return value == HEADER_HIGH
                || value == HEADER_LOW
                || value == ESCAPE
                || value == TAIL_HIGH
                || value == TAIL_LOW;
    }

    private static int checkType(int type) {
        switch (type) {
            case TYPE_DATA:
            case TYPE_END:
            case TYPE_ERROR:
            case TYPE_REQUEST:
                return type;
            default:
                throw new IllegalArgumentException(String.format("未知帧类型：0x%02X", type));
        }
    }

    private static long checkUnsignedInt(long value, String name) {
        if (value < 0 || value > 0xFFFFFFFFL) {
            throw new IllegalArgumentException(name + " 超出 4 字节无符号范围：" + value);
        }
        return value;
    }

    private static Header parseHeader(byte[] data) {
        if (data.length < PREFIX_LENGTH) {
            throw new IllegalArgumentException("帧头长度不足：" + data.length + " < " + PREFIX_LENGTH);
        }

        int version = readUnsignedShort(data, 2);
        if (version != VERSION) {
            throw new IllegalArgumentException(String.format("不支持的协议版本：0x%04X", version));
        }

        int compression = data[4] & 0xFF;
        if (compression != COMPRESSION_NONE) {
            throw new IllegalArgumentException("不支持的压缩算法：0x"
                    + String.format("%02X", compression));
        }

        int year = readUnsignedShort(data, 5);
        int month = data[7] & 0xFF;
        int day = data[8] & 0xFF;
        int hour = data[9] & 0xFF;
        int minute = data[10] & 0xFF;
        int second = data[11] & 0xFF;
        LocalDateTime timestamp;
        try {
            timestamp = LocalDateTime.of(year, month, day, hour, minute, second);
        } catch (DateTimeException e) {
            throw new IllegalArgumentException("时间戳非法："
                    + year + "-" + month + "-" + day + " "
                    + hour + ":" + minute + ":" + second, e);
        }
        ZoneOffset offset = decodeOffset(data[12]);
        int type = checkType(data[13] & 0xFF);
        long streamId = readUnsignedInt(data, 14);
        long sequence = readUnsignedInt(data, 18);
        long payloadLength = readUnsignedInt(data, 22);
        if (payloadLength > MAX_FRAME_LENGTH - FIXED_LENGTH) {
            throw new IllegalArgumentException("数据长度超过限制：" + payloadLength
                    + " > " + (MAX_FRAME_LENGTH - FIXED_LENGTH));
        }
        if (type == TYPE_END && payloadLength != 0) {
            throw new IllegalArgumentException("END 帧不能携带数据");
        }
        return new Header(version, compression, timestamp, offset, type, streamId, sequence, payloadLength);
    }

    private static final class Header {
        final int version;
        final int compressionAlgorithm;
        final LocalDateTime timestamp;
        final ZoneOffset timeZoneOffset;
        final int type;
        final long streamId;
        final long sequence;
        final long payloadLength;

        Header(int version, int compressionAlgorithm, LocalDateTime timestamp, ZoneOffset timeZoneOffset,
               int type, long streamId, long sequence, long payloadLength) {
            this.version = version;
            this.compressionAlgorithm = compressionAlgorithm;
            this.timestamp = timestamp;
            this.timeZoneOffset = timeZoneOffset;
            this.type = type;
            this.streamId = streamId;
            this.sequence = sequence;
            this.payloadLength = payloadLength;
        }
    }

    /**
     * 帧内增量读取器。
     *
     * <p>实例本身就是一个“反转义后的负载输入流”。每次 read 只从底层
     * 流读取当前需要的数据，因此可以在整个负载到达之前先处理已经收到的
     * 字节。读到 -1 时通信尾已经读取并校验，下一帧可以继续读取。</p>
     *
     * <p>如果调用方提前停止读取，应调用 {@link #close()} 丢弃当前帧
     * 剩余负载和通信尾，否则后续帧会错位；close 会阻塞到当前帧剩余
     * 数据全部读完。</p>
     */
    public static final class FrameReader extends InputStream {
        private final InputStream in;
        private final Header header;
        private final long wirePayloadLength;
        private long remainingWire;

        private final byte[] escapedBuffer = new byte[8192];
        private int escapedPos;
        private int escapedLimit;

        private final byte[] decodedBuffer = new byte[8192];
        private int decodedPos;
        private int decodedLimit;

        private boolean pendingEscape;
        private boolean finished;

        private FrameReader(InputStream in, Header header) {
            this.in = in;
            this.header = header;
            this.wirePayloadLength = header.payloadLength;
            this.remainingWire = header.payloadLength;
        }

        public int getVersion() {
            return header.version;
        }

        public int getCompressionAlgorithm() {
            return header.compressionAlgorithm;
        }

        public int getType() {
            return header.type;
        }

        public long getStreamId() {
            return header.streamId;
        }

        public long getSequence() {
            return header.sequence;
        }

        public LocalDateTime getTimestamp() {
            return header.timestamp;
        }

        public ZoneOffset getTimeZoneOffset() {
            return header.timeZoneOffset;
        }

        /**
         * @return 转义后的负载长度，即帧头中的数据长度字段
         */
        public long getWirePayloadLength() {
            return wirePayloadLength;
        }

        public boolean isFinished() {
            return finished;
        }

        @Override
        public int read() throws IOException {
            if (decodedPos == decodedLimit) {
                if (finished) {
                    return -1;
                }
                fillDecoded();
                if (decodedLimit == 0) {
                    return -1;
                }
            }
            return decodedBuffer[decodedPos++] & 0xFF;
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
            if (decodedPos == decodedLimit) {
                if (finished) {
                    return -1;
                }
                fillDecoded();
                if (decodedLimit == 0) {
                    return -1;
                }
            }
            int count = Math.min(len, decodedLimit - decodedPos);
            System.arraycopy(decodedBuffer, decodedPos, b, off, count);
            decodedPos += count;
            return count;
        }

        @Override
        public int available() {
            return decodedLimit - decodedPos;
        }

        /**
         * 丢弃当前帧剩余负载并消费通信尾，使流重新回到下一帧边界。
         * 如果已经读到 -1，则什么也不做。
         */
        @Override
        public void close() throws IOException {
            if (finished) {
                return;
            }
            byte[] discard = new byte[4096];
            while (read(discard, 0, discard.length) != -1) {
                // 丢弃当前帧剩余数据
            }
        }

        private void fillDecoded() throws IOException {
            decodedPos = 0;
            decodedLimit = 0;
            while (decodedLimit == 0 && !finished) {
                if (escapedPos == escapedLimit) {
                    if (remainingWire == 0) {
                        if (pendingEscape) {
                            throw new IllegalArgumentException("转义字节 0xCD 后缺少被转义的数据");
                        }
                        readTail();
                        return;
                    }
                    fillEscaped();
                }
                while (escapedPos < escapedLimit && decodedLimit < decodedBuffer.length) {
                    int value = escapedBuffer[escapedPos++] & 0xFF;
                    if (pendingEscape) {
                        if (!needsEscape(value)) {
                            throw new IllegalArgumentException(String.format(
                                    "非法转义序列 0xCD 0x%02X", value));
                        }
                        decodedBuffer[decodedLimit++] = (byte) value;
                        pendingEscape = false;
                    } else if (value == ESCAPE) {
                        pendingEscape = true;
                    } else if (needsEscape(value)) {
                        throw new IllegalArgumentException(String.format(
                                "特殊字节 0x%02X 未转义", value));
                    } else {
                        decodedBuffer[decodedLimit++] = (byte) value;
                    }
                }
            }
        }

        private void fillEscaped() throws IOException {
            int toRead = (int) Math.min(escapedBuffer.length, remainingWire);
            int read;
            do {
                read = in.read(escapedBuffer, 0, toRead);
            } while (read == 0);
            if (read == -1) {
                throw new EOFException("连接在数据帧接收完成前关闭");
            }
            escapedPos = 0;
            escapedLimit = read;
            remainingWire -= read;
        }

        private void readTail() throws IOException {
            byte[] tail = new byte[2];
            readFully(in, tail, 0, 2);
            int value = readUnsignedShort(tail, 0);
            if (value != TAIL) {
                throw new IllegalArgumentException(String.format("通信尾错误：0x%04X", value));
            }
            finished = true;
        }
    }

    /**
     * @return 按协议编码后的完整数据帧副本
     */
    public byte[] getBytes() {
        return bytes.clone();
    }

    public int getVersion() {
        return version;
    }

    public int getCompressionAlgorithm() {
        return compressionAlgorithm;
    }

    public int getType() {
        return type;
    }

    public long getStreamId() {
        return streamId;
    }

    public long getSequence() {
        return sequence;
    }

    /**
     * @return 时间戳中的本地日期时间部分（不含时区）
     */
    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    /**
     * @return 时间戳中的时区偏移量
     */
    public ZoneOffset getTimeZoneOffset() {
        return timeZoneOffset;
    }

    /**
     * @return 段落内容副本
     */
    public byte[] getPayload() {
        return payload.clone();
    }

    public int getPayloadLength() {
        return payload.length;
    }

    /**
     * 按 UTF-8 解码段落内容。
     *
     * <p>ERROR 帧的负载以 2 字节错误码开头；读取 ERROR 文本请使用
     * {@link #getErrorMessage()}，读取错误码请使用 {@link #getErrorCode()}。</p>
     */
    public String getPayloadAsString() {
        return new String(payload, StandardCharsets.UTF_8);
    }

    /**
     * @return ERROR 帧的 2 字节无符号错误码
     * @throws IllegalStateException 当前帧不是 ERROR 帧
     */
    public int getErrorCode() {
        requireErrorFrame();
        return readUnsignedShort(payload, 0);
    }

    /**
     * @return ERROR 帧的 UTF-8 错误信息
     * @throws IllegalStateException 当前帧不是 ERROR 帧
     */
    public String getErrorMessage() {
        requireErrorFrame();
        return new String(payload, 2, payload.length - 2, StandardCharsets.UTF_8);
    }

    private void requireErrorFrame() {
        if (type != TYPE_ERROR) {
            throw new IllegalStateException("当前帧不是 ERROR 帧");
        }
    }

    private static void validateErrorPayload(int type, byte[] payload) {
        if (type == TYPE_ERROR && payload.length < 2) {
            throw new IllegalArgumentException("ERROR 帧负载至少需要 2 字节错误码");
        }
    }

    /**
     * 协议中的时间戳只精确到秒，构造时先截掉纳秒，
     * 保证内存中的时间戳与编码到线上的内容一致；
     * 同时记录构造时刻的时区偏移量。
     */
    private static OffsetDateTime now() {
        return OffsetDateTime.now().withNano(0);
    }

    private static int readUnsignedShort(byte[] data, int offset) {
        return ((data[offset] & 0xFF) << 8) | (data[offset + 1] & 0xFF);
    }

    private static long readUnsignedInt(byte[] data, int offset) {
        return ((long) (data[offset] & 0xFF) << 24)
                | ((long) (data[offset + 1] & 0xFF) << 16)
                | ((long) (data[offset + 2] & 0xFF) << 8)
                | (data[offset + 3] & 0xFF);
    }

    private static void writeUnsignedInt(ByteArrayOutputStream out, long value) {
        out.write((int) ((value >>> 24) & 0xFF));
        out.write((int) ((value >>> 16) & 0xFF));
        out.write((int) ((value >>> 8) & 0xFF));
        out.write((int) (value & 0xFF));
    }

    private static void readFully(InputStream in, byte[] buffer, int offset, int length) throws IOException {
        int total = 0;
        while (total < length) {
            int read = in.read(buffer, offset + total, length - total);
            if (read == -1) {
                throw new EOFException("连接在数据帧接收完成前关闭");
            }
            total += read;
        }
    }

    @Override
    public String toString() {
        return String.format("SocketFrame{version=0x%04X, type=0x%02X, streamId=%d, sequence=%d, "
                        + "timestamp=%s%s, payloadLength=%d}",
                version, type, streamId, sequence, timestamp, timeZoneOffset, payload.length);
    }
}
