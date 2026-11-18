# StreamFrame

**English** | [简体中文](README.zh-CN.md)

[![Java 8+](https://img.shields.io/badge/Java-8%2B-blue.svg)](#requirements)
[![License: GPL-3.0](https://img.shields.io/badge/License-GPL--3.0-blue.svg)](LICENSE)
[![Protocol v3](https://img.shields.io/badge/protocol-v3-blueviolet.svg)](#protocol)

A small, dependency-free TCP protocol and Java library for **framed request/response with streaming output**.

StreamFrame gives you a reliable frame format, incremental payload reading, stream identifiers,
sequence numbers, and explicit `DATA` / `END` / `ERROR` frames, while keeping the everyday API simple:

```java
try (ProtocolConnection connection = ProtocolConnection.connect("127.0.0.1", 1818);
     ProtocolConnection.Response response = connection.request("hello")) {
    response.copyTo(System.out);
}
```

No framing code, no manual byte counts, no `END` bookkeeping in application code.

## Features

- Zero runtime dependencies; Java 8+.
- Length-prefixed binary frames with a magic header and tail.
- Request/response, plus multi-frame streaming responses.
- Automatic stream IDs, sequence numbers, `END` and `ERROR` handling.
- Incremental payload reading: process bytes before the whole frame arrives.
- UTF-8-friendly byte escaping for binary safety.
- Timezone-aware 8-byte timestamps (15-minute UTC-offset units).
- High-level `ProtocolConnection` facade and low-level `SocketFrame` codec.
- JUnit 5 test suite and runnable streaming demo.

## Requirements

- Java 8 or newer.
- Maven 3.6.3+ (for building).

## Build and test

Import the project as a Maven project in IntelliJ IDEA, Eclipse, or your preferred IDE.

```bash
mvn test
mvn package
```

The jar is written to `target/streamframe-0.1.0.jar`.

To install it into your local Maven repository:

```bash
mvn install
```

```xml
<dependency>
    <groupId>io.github.hopesdreams</groupId>
    <artifactId>streamframe</artifactId>
    <version>0.1.0</version>
</dependency>
```

> This repository targets the GitHub account `HopesDreams` and the repository name `streamframe`.
> Maven `groupId` and the Java package use lowercase `hopesdreams` by Java naming convention.
> If you fork under another account, update `pom.xml`, the package declarations, and both README files.

## Quick start

### Server

```java
import io.github.hopesdreams.streamframe.ProtocolConnection;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

try (ServerSocket server = new ServerSocket(1818)) {
    while (true) {
        Socket socket = server.accept();
        new Thread(() -> {
            try (ProtocolConnection connection = ProtocolConnection.of(socket)) {
                ProtocolConnection.Request request;
                while ((request = connection.receiveRequest()) != null) {
                    try (ProtocolConnection.Reply reply = request.reply()) {
                        reply.write("Hello, " + request.getText());
                        reply.write("\n");
                        reply.write("Goodbye!");
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();
    }
}
```

### Client

```java
import io.github.hopesdreams.streamframe.ProtocolConnection;

try (ProtocolConnection connection = ProtocolConnection.connect("127.0.0.1", 1818);
     ProtocolConnection.Response response = connection.request("hello")) {
    response.copyTo(System.out);
}
```

### Run the bundled demo

```bash
mvn package

java -cp target/streamframe-0.1.0.jar \
  io.github.hopesdreams.streamframe.demo.StreamingServer 1818

# in another terminal
java -cp target/streamframe-0.1.0.jar \
  io.github.hopesdreams.streamframe.demo.StreamingClient 127.0.0.1 1818
```

Expected client output:

```text
Response: Thank you for connecting: /127.0.0.1:1818
Goodbye!
```

The response is sent as three `DATA` frames followed by an `END` frame.

## High-level API

The main entry point is [`ProtocolConnection`](src/main/java/io/github/hopesdreams/streamframe/ProtocolConnection.java).

| API | Purpose |
|---|---|
| `ProtocolConnection.connect(host, port)` | Connect a client. |
| `ProtocolConnection.of(socket)` | Wrap an accepted server socket. |
| `request(String)` / `request(byte[])` | Send a `REQUEST` and return a streaming `Response`. |
| `receiveRequest()` | Read one complete `REQUEST`; returns `null` on clean EOF. |
| `Request.reply()` | Create the one allowed reply stream for a request. |
| `Reply.write(String)` / `write(byte[])` | Send one `DATA` frame (automatically chunked above 64 KiB). |
| `Reply.end()` / `Reply.close()` | Send `END`. |
| `Reply.error(String)` | Send `ERROR` and terminate the stream. |
| `Response.read(...)` | Read response bytes across frames. |
| `Response.copyTo(OutputStream)` | Stream the response to an output stream, flushing per chunk. |
| `Response.close()` | Drain the remaining response so the connection stays usable. |

Important behavior:

- A `Response` is a byte stream. It may span multiple `DATA` frames.
- Reading until `-1` means the server sent `END`.
- An `ERROR` frame is surfaced as an `IOException` containing the server message.
- Closing a partially-read `Response` drains the rest of that response.
- Requests and responses on one connection are sequential. Read or close the previous response
  before starting the next request.
- `ProtocolConnection` is not thread-safe.

## Low-level API

[`SocketFrame`](src/main/java/io/github/hopesdreams/streamframe/SocketFrame.java) exposes the frame codec
for custom protocols and tests.

```java
// Build a frame.
SocketFrame frame = SocketFrame.data(streamId, sequence, "chunk");
byte[] bytes = frame.getBytes();

// Parse a complete frame.
SocketFrame parsed = SocketFrame.readFrom(inputStream);

// Or read the payload incrementally.
SocketFrame.FrameReader reader = SocketFrame.openFrame(inputStream);
byte[] buffer = new byte[8192];
int n;
while ((n = reader.read(buffer)) != -1) {
    // process bytes as they arrive
}
// Reading -1 consumes and validates the frame tail.
```

Frame factories: `request`, `data`, `end`, `error`.
`FrameReader` is an `InputStream`; its `close()` drains the current frame so the next frame
stays aligned.

## Protocol

StreamFrame protocol version **3**. All multi-byte integers listed below are big-endian.

| Field | Value | Size |
|---|---|---|
| Magic header | `0xAA 0xBB` | 2 |
| Version | `0x00 0x03` | 2 |
| Compression | `0x00` = none | 1 |
| Timestamp | year(2), month, day, hour, minute, second, timezone | 8 |
| Frame type | `0x01` DATA, `0x02` END, `0x03` ERROR, `0x10` REQUEST | 1 |
| Stream ID | unsigned 32-bit | 4 |
| Sequence | unsigned 32-bit | 4 |
| Payload length | escaped payload bytes | 4 |
| Payload | escaped bytes | variable |
| Magic tail | `0xFF 0xEE` | 2 |

The fixed prefix before the payload is 26 bytes; an empty frame is 28 bytes including the tail.
The maximum encoded frame size is 1 MiB.

### Why a length field?

The frame length is known before the payload is read, so:

- stream IDs, sequence numbers, and payload bytes may contain `0xFF 0xEE` without being
  mistaken for the tail;
- the receiver reads exactly the right number of bytes;
- payload bytes can be processed before the entire frame arrives.

### Escaping

The payload is escaped before transmission. The escape byte is `0xCD`:

| Raw byte | Encoded |
|---|---|
| `0xAA` | `0xCD 0xAA` |
| `0xBB` | `0xCD 0xBB` |
| `0xCD` | `0xCD 0xCD` |
| `0xFF` | `0xCD 0xFF` |
| `0xEE` | `0xCD 0xEE` |

The payload length field stores the escaped length. Header, version, compression, timestamp,
type, stream ID, sequence, length, and tail are not escaped.

### Timestamp and timezone

The timestamp stores local civil time plus a one-byte UTC offset in **15-minute units**, as a
signed two's-complement byte:

| Byte | Offset |
|---|---|
| `0x00` | UTC+00:00 |
| `0x04` | UTC+01:00 |
| `0x20` | UTC+08:00 |
| `0xE0` | UTC-08:00 |
| `0x16` | UTC+05:30 |
| `0x17` | UTC+05:45 |

This covers whole-hour, half-hour, and 45-minute timezones.

### Streams and sequence numbers

- A client starts a stream with a `REQUEST` frame.
- The server replies with `DATA` frames and terminates with `END` (or `ERROR`).
- `END` must have an empty payload.
- Stream IDs and sequence numbers are unsigned 32-bit values.
- Sequence numbers start at 0 per stream and direction, increment by one per frame, and wrap
  from `0xFFFFFFFF` to `0`.

## Error handling and limits

- I/O and high-level protocol failures are reported as `IOException`.
- Low-level invalid frame construction/parsing throws `IllegalArgumentException`.
- A server `ERROR` frame reaches a high-level client as an `IOException` with the server text.
- Compression is reserved; only `0x00` (none) is supported.
- One encoded frame is capped at 1 MiB. `Reply.write` automatically splits large data into
  multiple `DATA` frames.
- The library does not provide TLS, authentication, heartbeats, retries, or multiplexing.
  Put it behind TLS or add those features in your application.

## Project layout

```text
src/main/java/io/github/hopesdreams/streamframe/
├── ProtocolConnection.java   high-level API
├── SocketFrame.java          frame codec and incremental reader
├── package-info.java
└── demo/
    ├── StreamingServer.java
    ├── StreamingClient.java
    └── package-info.java
src/test/java/io/github/hopesdreams/streamframe/
├── ProtocolConnectionTest.java
└── SocketFrameTest.java
```

## Contributing

Issues and pull requests are welcome. Please run `mvn test` before submitting a change.
Keep the wire format backward compatible or bump the protocol version.

## License

This project is licensed under the **GNU General Public License v3.0 or later**
(GPL-3.0-or-later). See [LICENSE](LICENSE) for the full text and
[NOTICE](NOTICE) for the copyright notice.

GPL is a copyleft license: if you distribute a modified version, you must make the
corresponding source code available under the same license terms.

## Chinese documentation

A full Chinese version is available at [README.zh-CN.md](README.zh-CN.md).
