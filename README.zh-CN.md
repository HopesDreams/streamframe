# StreamFrame

[English](README.md) | **简体中文**

[![Java 8+](https://img.shields.io/badge/Java-8%2B-blue.svg)](#环境要求)
[![License: GPL-3.0](https://img.shields.io/badge/License-GPL--3.0-blue.svg)](LICENSE)
[![Protocol v3](https://img.shields.io/badge/protocol-v3-blueviolet.svg)](#协议)

一个零运行时依赖、面向 Java 8+ 的 TCP 帧协议与 Java 库，支持**带流式输出的请求/响应**。

StreamFrame 提供可靠的帧格式、帧内增量读取、流 ID、序号，以及明确的 `DATA` / `END` / `ERROR` 帧。
日常调用可以非常简单：

```java
try (ProtocolConnection connection = ProtocolConnection.connect("127.0.0.1", 1818);
     ProtocolConnection.Response response = connection.request("hello")) {
    response.copyTo(System.out);
}
```

业务代码不需要处理帧头、字节计数或 END 帧。

## 特性

- 零运行时依赖，支持 Java 8+。
- 带长度前缀的二进制帧，包含固定通信头和通信尾。
- 支持请求/响应和多帧流式响应。
- 自动分配流 ID、维护序号、发送 END/ERROR。
- 支持帧内增量读取：整帧尚未到达时就可以处理已收到的数据。
- 对二进制安全：使用 `0xCD` 转义特殊字节。
- 8 字节时间戳，带时区偏移（以 15 分钟为单位）。
- 提供高层 `ProtocolConnection` 门面和底层 `SocketFrame` 编解码器。
- 附带 JUnit 5 测试和可直接运行的流式示例。

## 环境要求

- Java 8 或更高版本。
- Maven 3.6.3+（仅构建需要）。

## 构建与测试

在 IntelliJ IDEA、Eclipse 或常用 IDE 中按 Maven 项目导入即可。

```bash
mvn test
mvn package
```

生成的 jar 位于 `target/streamframe-0.1.0.jar`。

安装到本地 Maven 仓库：

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

> 本仓库面向 GitHub 账号 `HopesDreams`，仓库名为 `streamframe`。
> Maven `groupId` 和 Java 包名按 Java 命名惯例使用小写 `hopesdreams`。
> 如果要在其他账号下发布，请同步修改 `pom.xml`、源码包名和两个 README 文件。

## 快速开始

### 服务端

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

### 客户端

```java
import io.github.hopesdreams.streamframe.ProtocolConnection;

try (ProtocolConnection connection = ProtocolConnection.connect("127.0.0.1", 1818);
     ProtocolConnection.Response response = connection.request("hello")) {
    response.copyTo(System.out);
}
```

### 运行自带示例

```bash
mvn package

java -cp target/streamframe-0.1.0.jar \
  io.github.hopesdreams.streamframe.demo.StreamingServer 1818

# 另开一个终端
java -cp target/streamframe-0.1.0.jar \
  io.github.hopesdreams.streamframe.demo.StreamingClient 127.0.0.1 1818
```

客户端预期输出：

```text
Response: Thank you for connecting: /127.0.0.1:1818
Goodbye!
```

服务端实际会发送 3 个 `DATA` 帧，最后发送 `END` 帧。

## 高层 API

主要入口是 [`ProtocolConnection`](src/main/java/io/github/hopesdreams/streamframe/ProtocolConnection.java)。

| API | 用途 |
|---|---|
| `ProtocolConnection.connect(host, port)` | 客户端建立连接。 |
| `ProtocolConnection.of(socket)` | 包装服务端已接受的 Socket。 |
| `request(String)` / `request(byte[])` | 发送 `REQUEST`，返回流式 `Response`。 |
| `receiveRequest()` | 读取一个完整 `REQUEST`；正常 EOF 时返回 `null`。 |
| `Request.reply()` | 为请求创建唯一一次回复流。 |
| `Reply.write(String)` / `write(byte[])` | 发送一个 `DATA` 帧；超过 64 KiB 自动分块。 |
| `Reply.end()` / `Reply.close()` | 发送 `END`。 |
| `Reply.error(String)` | 发送 `ERROR` 并结束流。 |
| `Response.read(...)` | 跨帧读取响应字节。 |
| `Response.copyTo(OutputStream)` | 边收边写响应，每块立即 flush。 |
| `Response.close()` | 丢弃剩余响应，使连接回到可用状态。 |

重要行为：

- `Response` 是字节流，可能横跨多个 `DATA` 帧。
- 读取返回 `-1` 表示收到 `END`。
- 收到 `ERROR` 帧时，会抛出包含服务端错误文本的 `IOException`。
- 提前关闭未读完的 `Response` 时，会读取并丢弃该响应的剩余内容。
- 同一条连接上的请求/响应按顺序使用：读完或关闭上一个 `Response` 后再发下一个请求。
- `ProtocolConnection` 不是线程安全的。

## 底层 API

[`SocketFrame`](src/main/java/io/github/hopesdreams/streamframe/SocketFrame.java) 提供底层帧编解码能力，
适合自定义协议或测试。

```java
// 构造帧
SocketFrame frame = SocketFrame.data(streamId, sequence, "chunk");
byte[] bytes = frame.getBytes();

// 解析完整帧
SocketFrame parsed = SocketFrame.readFrom(inputStream);

// 或增量读取负载
SocketFrame.FrameReader reader = SocketFrame.openFrame(inputStream);
byte[] buffer = new byte[8192];
int n;
while ((n = reader.read(buffer)) != -1) {
    // 边收边处理
}
// 读到 -1 时通信尾已经校验并消费
```

工厂方法：`request`、`data`、`end`、`error`。

`FrameReader` 本身是 `InputStream`；它的 `close()` 会丢弃当前帧剩余内容，
保证下一帧仍然对齐。

## 协议

StreamFrame 协议版本为 **3**。下表所有多字节整数均为大端序。

| 字段 | 值 | 长度 |
|---|---|---|
| 通信头 | `0xAA 0xBB` | 2 |
| 版本号 | `0x00 0x03` | 2 |
| 压缩算法 | `0x00` 表示不压缩 | 1 |
| 时间戳 | 年(2)、月、日、时、分、秒、时区 | 8 |
| 帧类型 | `0x01` DATA、`0x02` END、`0x03` ERROR、`0x10` REQUEST | 1 |
| 流 ID | 无符号 32 位 | 4 |
| 序号 | 无符号 32 位 | 4 |
| 数据长度 | 转义后的负载字节数 | 4 |
| 段落内容 | 转义后的数据 | 变长 |
| 通信尾 | `0xFF 0xEE` | 2 |

负载之前的固定前缀为 26 字节；空负载帧包含通信尾共 28 字节。
单帧编码后的最大长度为 1 MiB。

### 为什么需要长度字段？

接收方在读取负载之前就知道长度，因此：

- 流 ID、序号和负载中出现 `0xFF 0xEE` 时不会被误判为通信尾；
- 接收方可以精确读取对应字节数；
- 负载可以在整帧到达之前被逐块处理。

### 转义规则

负载发送前会转义，转义字节为 `0xCD`：

| 原始字节 | 编码后 |
|---|---|
| `0xAA` | `0xCD 0xAA` |
| `0xBB` | `0xCD 0xBB` |
| `0xCD` | `0xCD 0xCD` |
| `0xFF` | `0xCD 0xFF` |
| `0xEE` | `0xCD 0xEE` |

数据长度字段保存的是**转义后**的负载长度。通信头、版本号、压缩算法、时间戳、
帧类型、流 ID、序号、长度字段和通信尾本身不参与转义。

### 时间戳与时区

时间戳保存本地时间和 1 字节 UTC 偏移；偏移以 **15 分钟**为单位，
使用 8 位有符号补码：

| 字节 | 偏移 |
|---|---|
| `0x00` | UTC+00:00 |
| `0x04` | UTC+01:00 |
| `0x20` | UTC+08:00 |
| `0xE0` | UTC-08:00 |
| `0x16` | UTC+05:30 |
| `0x17` | UTC+05:45 |

可覆盖整点、半小时和 45 分钟时区。

### 流与序号

- 客户端通过 `REQUEST` 帧发起一个流。
- 服务端通过多个 `DATA` 帧回复，最后发送 `END`（或 `ERROR`）。
- `END` 帧的负载必须为空。
- 流 ID 和序号均为无符号 32 位整数。
- 同一流、同一方向内序号从 0 开始逐帧加 1，到 `0xFFFFFFFF` 后回绕到 0。

## 错误处理与限制

- I/O 和高层协议错误以 `IOException` 抛出。
- 低层帧构造/解析错误以 `IllegalArgumentException` 抛出。
- 服务端 `ERROR` 帧在高层客户端表现为携带服务端错误文本的 `IOException`。
- 压缩算法字段目前仅支持 `0x00`（不压缩）。
- 单帧编码后上限为 1 MiB；`Reply.write` 会自动把大数据拆成多个 `DATA` 帧。
- 本库不提供 TLS、认证、心跳、重试或多路复用。生产环境请置于 TLS 之后，
  或在应用层补充这些能力。

## 项目结构

```text
src/main/java/io/github/hopesdreams/streamframe/
├── ProtocolConnection.java   高层 API
├── SocketFrame.java          帧编解码与增量读取
├── package-info.java
└── demo/
    ├── StreamingServer.java
    ├── StreamingClient.java
    └── package-info.java
src/test/java/io/github/hopesdreams/streamframe/
├── ProtocolConnectionTest.java
└── SocketFrameTest.java
```

## 贡献

欢迎提交 Issue 和 Pull Request。提交前请先运行 `mvn test`。
修改线上格式时请保持向后兼容，或升级协议版本。

## 许可证

本项目采用 **GNU General Public License v3.0 or later（GPL-3.0-or-later）**。
完整文本见 [LICENSE](LICENSE)，版权声明见 [NOTICE](NOTICE)。

GPL 是 Copyleft 许可证：如果分发修改后的版本，必须以相同许可证条款提供对应源码。
