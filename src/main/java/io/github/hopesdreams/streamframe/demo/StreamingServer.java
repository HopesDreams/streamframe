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

import java.io.EOFException;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Small streaming demo server.
 *
 * <p>Usage: {@code java ... StreamingServer [port]}</p>
 *
 * <p>The server accepts requests and answers each one with several DATA
 * frames followed by an END frame. It is intentionally simple; production
 * servers should add authentication, TLS, connection limits and timeouts.</p>
 */
public final class StreamingServer {
    private static final int DEFAULT_PORT = 1818;

    private StreamingServer() {
    }

    public static void main(String[] args) throws IOException {
        int port = parsePort(args);
        ExecutorService workers = Executors.newCachedThreadPool();

        try (ServerSocket server = new ServerSocket(port)) {
            System.out.println("StreamFrame server listening on port " + port);
            while (true) {
                Socket socket = server.accept();
                workers.submit(new ConnectionHandler(socket));
            }
        } finally {
            workers.shutdownNow();
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

    private static final class ConnectionHandler implements Runnable {
        private final Socket socket;

        private ConnectionHandler(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            String remote = String.valueOf(socket.getRemoteSocketAddress());
            try (ProtocolConnection connection = ProtocolConnection.of(socket)) {
                ProtocolConnection.Request request;
                while ((request = connection.receiveRequest()) != null) {
                    byte[] payload = request.getPayload();
                    System.out.println("Request from " + remote
                            + " (stream " + request.getStreamId()
                            + ", " + payload.length + " bytes): "
                            + new String(payload, StandardCharsets.UTF_8));

                    try (ProtocolConnection.Reply reply = request.reply()) {
                        reply.write("Thank you for connecting: " + socket.getLocalSocketAddress());
                        reply.write("\n");
                        reply.write("Goodbye!");
                    }
                }
                System.out.println("Client disconnected: " + remote);
            } catch (EOFException e) {
                System.out.println("Client disconnected: " + remote);
            } catch (IOException e) {
                System.out.println("Connection error (" + remote + "): " + e.getMessage());
            }
        }
    }
}
