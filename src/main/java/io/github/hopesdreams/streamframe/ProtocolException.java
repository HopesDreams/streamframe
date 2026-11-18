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

import java.io.IOException;

/**
 * An {@link IOException} raised when the remote peer sends an ERROR frame.
 *
 * <p>The numeric code is available via {@link #getErrorCode()}; use
 * {@link ErrorCode#nameOf(int)} for a display name. The raw server message is
 * available via {@link #getServerMessage()}.</p>
 */
public class ProtocolException extends IOException {
    private static final long serialVersionUID = 1L;

    private final int errorCode;
    private final String serverMessage;

    public ProtocolException(int errorCode, String serverMessage) {
        super("服务端错误 0x" + String.format("%04X", errorCode & 0xFFFF)
                + " (" + ErrorCode.nameOf(errorCode) + "): " + serverMessage);
        this.errorCode = errorCode & 0xFFFF;
        this.serverMessage = serverMessage == null ? "" : serverMessage;
    }

    /**
     * @return the numeric error code from the ERROR frame
     */
    public int getErrorCode() {
        return errorCode;
    }

    /**
     * @return the display name of the error code
     */
    public String getErrorName() {
        return ErrorCode.nameOf(errorCode);
    }

    /**
     * @return the UTF-8 message supplied by the remote peer
     */
    public String getServerMessage() {
        return serverMessage;
    }

    /**
     * @return whether the error is transient and an idempotent operation may
     *         be retried
     */
    public boolean isRetryable() {
        return ErrorCode.isRetryable(errorCode);
    }
}
