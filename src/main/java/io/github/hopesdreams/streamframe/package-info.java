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
/**
 * StreamFrame: a small framed TCP protocol with request/response and
 * streaming response support for Java 8+.
 *
 * <p>{@link io.github.hopesdreams.streamframe.ProtocolConnection} is the high-level
 * API; {@link io.github.hopesdreams.streamframe.SocketFrame} is the low-level frame
 * codec. ERROR frames use the standard codes in
 * {@link io.github.hopesdreams.streamframe.ErrorCode} and surface as
 * {@link io.github.hopesdreams.streamframe.ProtocolException}.</p>
 */
package io.github.hopesdreams.streamframe;
