/*
 * Copyright 2024-2026 The x-raft-lib Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.xinfra.lab.raft.internal;

import io.github.xinfra.lab.raft.RaftLogger;
import java.util.concurrent.TimeUnit;

/**
 * Time-based log suppression for high-frequency events on the raft event loop.
 *
 * <p>Not thread-safe — designed for single-threaded use on the raft event loop.
 */
final class RateLimitedLog {

    static final long DEFAULT_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(10);

    private final RaftLogger logger;
    private final long intervalNanos;
    private long lastLogNanos;
    private long suppressed;
    private boolean hasLogged;

    RateLimitedLog(RaftLogger logger) {
        this(logger, DEFAULT_INTERVAL_NANOS);
    }

    RateLimitedLog(RaftLogger logger, long intervalNanos) {
        this.logger = logger;
        this.intervalNanos = intervalNanos;
    }

    void info(String format, Object... args) {
        if (shouldLog()) {
            logger.info(format, args);
        }
    }

    void warn(String format, Object... args) {
        if (shouldLog()) {
            logger.warn(format, args);
        }
    }

    void flush() {
        if (suppressed > 0) {
            logger.info("(suppressed {} proposal-drop messages)", suppressed);
            suppressed = 0;
        }
    }

    private boolean shouldLog() {
        long now = System.nanoTime();
        if (!hasLogged || now - lastLogNanos >= intervalNanos) {
            if (suppressed > 0) {
                logger.info("(suppressed {} proposal-drop messages)", suppressed);
                suppressed = 0;
            }
            lastLogNanos = now;
            hasLogged = true;
            return true;
        }
        suppressed++;
        return false;
    }

    long suppressed() {
        return suppressed;
    }
}
