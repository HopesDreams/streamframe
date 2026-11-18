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
package io.github.hopesdreams.streamframe.demo;

import io.github.hopesdreams.streamframe.ProtocolConnection;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Small streaming demo server.
 *
 * <p>Usage: {@code java ... StreamingServer [port]}</p>
 *
 * <p>The server accepts requests and answers each one with several DATA
 * frames followed by an END frame. It uses a bounded worker pool, a maximum
 * connection count, an idle read timeout, and a graceful shutdown path. These
 * limits are intentionally conservative; tune the constants for your workload.</p>
 *
 * <p>Note: Java blocking sockets do not expose a write timeout. A client that
 * stops reading is handled by TCP backpressure; shutting down the server closes
 * active sockets and unblocks worker threads.</p>
 */
public final class StreamingServer {
    private static final int DEFAULT_PORT = 1818;

    /** Listen backlog passed to {@link ServerSocket#bind}. */
    private static final int BACKLOG = 256;

    /** Maximum number of accepted, not-yet-finished connections. */
    private static final int MAX_CONNECTIONS = 256;

    /** Bounded worker pool: core threads, maximum threads, and queue capacity. */
    private static final int CORE_POOL_SIZE = 16;
    private static final int MAX_POOL_SIZE = 64;
    private static final int WORK_QUEUE_CAPACITY = 128;
    private static final long KEEP_ALIVE_SECONDS = 60L;

    /** Idle read timeout for a connection, in milliseconds. */
    private static final int READ_TIMEOUT_MILLIS = 30_000;

    /** How long shutdown waits for worker threads to finish. */
    private static final long SHUTDOWN_TIMEOUT_SECONDS = 5L;

    private final ServerSocket server;
    private final ThreadPoolExecutor workers;
    private final Semaphore connectionSlots = new Semaphore(MAX_CONNECTIONS);
    private final Set<Socket> activeSockets = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean stopped = new AtomicBoolean();

    private StreamingServer(int port) throws IOException {
        ServerSocket serverSocket = new ServerSocket();
        try {
            serverSocket.setReuseAddress(true);
            serverSocket.bind(new InetSocketAddress(port), BACKLOG);
        } catch (IOException e) {
            closeQuietly(serverSocket);
            throw e;
        }

        this.server = serverSocket;
        this.workers = new ThreadPoolExecutor(
                CORE_POOL_SIZE,
                MAX_POOL_SIZE,
                KEEP_ALIVE_SECONDS,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(WORK_QUEUE_CAPACITY),
                new WorkerThreadFactory(),
                new ThreadPoolExecutor.AbortPolicy());
        this.workers.allowCoreThreadTimeOut(true);
    }

    public static void main(String[] args) throws IOException {
        int port = parsePort(args);
        StreamingServer server = new StreamingServer(port);
        Runtime.getRuntime().addShutdownHook(
                new Thread(server::shutdown, "streamframe-shutdown"));
        server.run();
    }

    /**
     * Accept connections until {@link #shutdown()} is called.
     */
    public void run() throws IOException {
        System.out.println("StreamFrame server listening on port " + server.getLocalPort());
        try {
            while (!stopped.get()) {
                Socket socket;
                try {
                    socket = server.accept();
                } catch (SocketException e) {
                    if (stopped.get()) {
                        break;
                    }
                    throw e;
                }

                if (stopped.get()) {
                    closeQuietly(socket);
                    break;
                }
                handleNewConnection(socket);
            }
        } finally {
            shutdown();
        }
    }

    /**
     * Stop accepting connections, wait up to the shutdown timeout for in-flight
     * work, then close remaining active sockets and stop the worker pool.
     */
    public void shutdown() {
        if (!stopped.compareAndSet(false, true)) {
            return;
        }

        System.out.println("Shutting down StreamFrame server...");
        closeQuietly(server);

        // 第一阶段：不再接收新任务，给正在处理的请求一点时间自然结束。
        workers.shutdown();
        boolean terminated = false;
        try {
            terminated = workers.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // 第二阶段：仍有连接未结束时，关闭活动 Socket 并强制停止。
        if (!terminated) {
            System.out.println("Graceful shutdown timed out; closing active connections");
            for (Socket socket : activeSockets) {
                closeQuietly(socket);
            }
            workers.shutdownNow();
            try {
                if (!workers.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                    System.out.println("Worker pool did not stop within "
                            + SHUTDOWN_TIMEOUT_SECONDS + "s");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        activeSockets.clear();
        System.out.println("StreamFrame server stopped");
    }

    private void handleNewConnection(Socket socket) {
        // Connection-level overload: no REQUEST has been read yet, so there is
        // no stream to attach an ERROR frame to. Close the socket and let
        // clients retry idempotent requests with backoff.
        if (!connectionSlots.tryAcquire()) {
            System.out.println("Rejected connection " + socket.getRemoteSocketAddress()
                    + ": connection limit reached (" + MAX_CONNECTIONS + ")");
            closeQuietly(socket);
            return;
        }

        activeSockets.add(socket);
        try {
            workers.execute(new ConnectionHandler(socket));
            if (stopped.get()) {
                // shutdown() has already iterated activeSockets; make sure this
                // late connection cannot stay open.
                closeQuietly(socket);
            }
        } catch (RejectedExecutionException e) {
            // Same load-shedding policy as the connection limit above.
            activeSockets.remove(socket);
            connectionSlots.release();
            closeQuietly(socket);
            System.out.println("Rejected connection " + socket.getRemoteSocketAddress()
                    + ": worker queue full");
        }
    }

    private static int parsePort(String[] args) {
        if (args.length == 0) {
            return DEFAULT_PORT;
        }
        if (args.length > 1) {
            throw new IllegalArgumentException("Usage: StreamingServer [port]");
        }
        int port;
        try {
            port = Integer.parseInt(args[0]);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid port: " + args[0], e);
        }
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("Port must be in 1..65535: " + port);
        }
        return port;
    }

    private static void closeQuietly(Closeable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (IOException ignored) {
            // 忽略关闭失败
        }
    }

    private final class ConnectionHandler implements Runnable {
        private final Socket socket;

        private ConnectionHandler(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            String remote = String.valueOf(socket.getRemoteSocketAddress());
            try {
                socket.setSoTimeout(READ_TIMEOUT_MILLIS);
                socket.setKeepAlive(true);

                try (ProtocolConnection connection = ProtocolConnection.of(socket)) {
                    ProtocolConnection.Request request;
                    while ((request = connection.receiveRequest()) != null) {
                        byte[] payload = request.getPayload();
                        System.out.println("Request from " + remote
                                + " (stream " + request.getStreamId()
                                + ", " + payload.length + " bytes): "
                                + new String(payload, StandardCharsets.UTF_8));

                        try (ProtocolConnection.Reply reply = request.reply()) {
                            reply.write("Thank you for connecting: "
                                    + socket.getLocalSocketAddress());
                            reply.write("\n");
                            reply.write("Goodbye!");
                        }
                    }
                    System.out.println("Client disconnected: " + remote);
                }
            } catch (SocketTimeoutException e) {
                System.out.println("Connection timeout: " + remote);
            } catch (EOFException e) {
                System.out.println("Client disconnected: " + remote);
            } catch (IOException e) {
                if (!stopped.get()) {
                    System.out.println("Connection error (" + remote + "): " + e.getMessage());
                }
            } finally {
                activeSockets.remove(socket);
                connectionSlots.release();
                closeQuietly(socket);
            }
        }
    }

    private static final class WorkerThreadFactory implements ThreadFactory {
        private final AtomicInteger counter = new AtomicInteger();

        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread = new Thread(runnable,
                    "streamframe-worker-" + counter.incrementAndGet());
            thread.setDaemon(false);
            return thread;
        }
    }
}
