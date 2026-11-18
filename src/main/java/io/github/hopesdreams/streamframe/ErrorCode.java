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

/**
 * Standard error codes carried by {@link SocketFrame#TYPE_ERROR} frames.
 *
 * <p>An ERROR payload starts with a two-byte unsigned big-endian error code,
 * followed by a UTF-8 message. Codes {@code 0x0000} through {@code 0x0008}
 * are defined by this library; applications may use other values in the
 * {@code 0x0100-0xFFFF} range for their own errors.</p>
 */
public final class ErrorCode {
    /** No specific error was reported. */
    public static final int UNSPECIFIED = 0x0000;

    /** Malformed frame, invalid sequence, or another wire-level violation. */
    public static final int PROTOCOL_ERROR = 0x0001;

    /** A well-formed request was rejected by the server. */
    public static final int INVALID_REQUEST = 0x0002;

    /** The server is overloaded or out of connection/worker capacity. */
    public static final int SERVER_BUSY = 0x0003;

    /** Unexpected server-side failure. */
    public static final int INTERNAL_ERROR = 0x0004;

    /** The operation or connection timed out. */
    public static final int TIMEOUT = 0x0005;

    /** The stream was cancelled or aborted before END. */
    public static final int STREAM_ABORTED = 0x0006;

    /** The requested version, feature, or operation is not supported. */
    public static final int UNSUPPORTED = 0x0007;

    /** The request or response payload exceeds the allowed size. */
    public static final int PAYLOAD_TOO_LARGE = 0x0008;

    private ErrorCode() {
    }

    /**
     * Returns whether a failed operation may be retried when it is idempotent.
     *
     * <p>{@link #SERVER_BUSY} and {@link #TIMEOUT} are considered transient.
     * Other codes usually indicate a request/protocol problem that will not be
     * fixed by retrying.</p>
     */
    public static boolean isRetryable(int code) {
        return code == SERVER_BUSY || code == TIMEOUT;
    }

    /**
     * Returns a stable display name for a known code, or an {@code UNKNOWN}
     * label for application-defined codes.
     */
    public static String nameOf(int code) {
        switch (code) {
            case UNSPECIFIED:
                return "UNSPECIFIED";
            case PROTOCOL_ERROR:
                return "PROTOCOL_ERROR";
            case INVALID_REQUEST:
                return "INVALID_REQUEST";
            case SERVER_BUSY:
                return "SERVER_BUSY";
            case INTERNAL_ERROR:
                return "INTERNAL_ERROR";
            case TIMEOUT:
                return "TIMEOUT";
            case STREAM_ABORTED:
                return "STREAM_ABORTED";
            case UNSUPPORTED:
                return "UNSUPPORTED";
            case PAYLOAD_TOO_LARGE:
                return "PAYLOAD_TOO_LARGE";
            default:
                return String.format("UNKNOWN(0x%04X)", code & 0xFFFF);
        }
    }
}
