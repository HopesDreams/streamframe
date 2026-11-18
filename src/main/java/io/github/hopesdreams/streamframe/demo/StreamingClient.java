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

/**
 * Small streaming demo client.
 *
 * <p>Usage: {@code java ... StreamingClient [host] [port]}</p>
 */
public final class StreamingClient {
    private static final String DEFAULT_HOST = "127.0.0.1";
    private static final int DEFAULT_PORT = 1818;

    private StreamingClient() {
    }

    public static void main(String[] args) {
        if (args.length > 2) {
            throw new IllegalArgumentException("Usage: StreamingClient [host] [port]");
        }
        String host = args.length > 0 ? args[0] : DEFAULT_HOST;
        int port = args.length > 1 ? parsePort(args[1]) : DEFAULT_PORT;

        try (ProtocolConnection connection = ProtocolConnection.connect(host, port);
             ProtocolConnection.Response response = connection.request(
                     "Hello from " + connection.getLocalSocketAddress())) {

            System.out.print("Response: ");
            response.copyTo(System.out);
            System.out.println();
        } catch (EOFException e) {
            System.out.println("\nServer closed the connection before END");
        } catch (IOException e) {
            System.out.println("\nConnection error: " + e.getMessage());
        }
    }

    private static int parsePort(String value) {
        int port;
        try {
            port = Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid port: " + value, e);
        }
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("Port must be in 1..65535: " + port);
        }
        return port;
    }
}
