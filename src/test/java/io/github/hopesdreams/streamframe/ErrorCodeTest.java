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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ErrorCodeTest {

    @Test
    void identifiesRetryableCodes() {
        assertTrue(ErrorCode.isRetryable(ErrorCode.SERVER_BUSY));
        assertTrue(ErrorCode.isRetryable(ErrorCode.TIMEOUT));
        assertFalse(ErrorCode.isRetryable(ErrorCode.INVALID_REQUEST));
        assertFalse(ErrorCode.isRetryable(ErrorCode.PROTOCOL_ERROR));
    }

    @Test
    void exposesProtocolExceptionDetails() {
        ProtocolException exception = new ProtocolException(ErrorCode.SERVER_BUSY, "busy");

        assertEquals(ErrorCode.SERVER_BUSY, exception.getErrorCode());
        assertEquals("SERVER_BUSY", exception.getErrorName());
        assertEquals("busy", exception.getServerMessage());
        assertTrue(exception.isRetryable());
        assertTrue(exception.getMessage().contains("SERVER_BUSY"));
        assertTrue(exception.getMessage().contains("busy"));
    }

    @Test
    void namesKnownAndUnknownCodes() {
        assertEquals("UNSPECIFIED", ErrorCode.nameOf(ErrorCode.UNSPECIFIED));
        assertEquals("SERVER_BUSY", ErrorCode.nameOf(ErrorCode.SERVER_BUSY));
        assertEquals("UNKNOWN(0x0100)", ErrorCode.nameOf(0x0100));
    }
}
