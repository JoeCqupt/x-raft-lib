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
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.*;

class RateLimitedLogTest {

    static class RecordingLogger implements RaftLogger {
        final List<String> messages = new ArrayList<>();

        @Override public void debug(String format, Object... args) { messages.add("DEBUG: " + format); }
        @Override public void info(String format, Object... args) { messages.add("INFO: " + format); }
        @Override public void warn(String format, Object... args) { messages.add("WARN: " + format); }
        @Override public void error(String format, Object... args) { messages.add("ERROR: " + format); }
        @Override public void fatal(String format, Object... args) { messages.add("FATAL: " + format); }
        @Override public void panic(String format, Object... args) { messages.add("PANIC: " + format); }
    }

    @Test
    void firstCallAlwaysLogs() {
        RecordingLogger logger = new RecordingLogger();
        RateLimitedLog rl = new RateLimitedLog(logger, TimeUnit.HOURS.toNanos(1));

        rl.info("hello");
        assertThat(logger.messages).containsExactly("INFO: hello");
        assertThat(rl.suppressed()).isZero();
    }

    @Test
    void subsequentCallsWithinIntervalAreSuppressed() {
        RecordingLogger logger = new RecordingLogger();
        RateLimitedLog rl = new RateLimitedLog(logger, TimeUnit.HOURS.toNanos(1));

        rl.info("first");
        rl.info("second");
        rl.info("third");
        rl.warn("fourth");

        assertThat(logger.messages).containsExactly("INFO: first");
        assertThat(rl.suppressed()).isEqualTo(3);
    }

    @Test
    void flushEmitsSuppressedCount() {
        RecordingLogger logger = new RecordingLogger();
        RateLimitedLog rl = new RateLimitedLog(logger, TimeUnit.HOURS.toNanos(1));

        rl.info("first");
        rl.info("suppressed-1");
        rl.info("suppressed-2");

        rl.flush();
        assertThat(logger.messages).hasSize(2);
        assertThat(logger.messages.get(1)).contains("suppressed");
        assertThat(rl.suppressed()).isZero();
    }

    @Test
    void flushWithNothingSuppressedIsNoOp() {
        RecordingLogger logger = new RecordingLogger();
        RateLimitedLog rl = new RateLimitedLog(logger, TimeUnit.HOURS.toNanos(1));

        rl.flush();
        assertThat(logger.messages).isEmpty();

        rl.info("first");
        rl.flush();
        assertThat(logger.messages).containsExactly("INFO: first");
    }

    @Test
    void afterIntervalExpiredLogsAgain() {
        RecordingLogger logger = new RecordingLogger();
        RateLimitedLog rl = new RateLimitedLog(logger, 1);

        rl.info("first");
        // Interval is 1 nanosecond — by the time we call again it will have expired
        rl.info("second");

        assertThat(logger.messages).contains("INFO: first", "INFO: second");
    }

    @Test
    void warnUsesWarnLevel() {
        RecordingLogger logger = new RecordingLogger();
        RateLimitedLog rl = new RateLimitedLog(logger, TimeUnit.HOURS.toNanos(1));

        rl.warn("warning");
        assertThat(logger.messages).containsExactly("WARN: warning");
    }

    @Test
    void suppressedCountReportedOnNextAllowedLog() {
        RecordingLogger logger = new RecordingLogger();

        RateLimitedLog rl = new RateLimitedLog(logger, TimeUnit.HOURS.toNanos(1));
        rl.info("a");
        rl.info("b");
        rl.info("c");
        assertThat(rl.suppressed()).isEqualTo(2);

        rl.flush();
        assertThat(logger.messages).hasSize(2);
        assertThat(logger.messages.get(1)).contains("suppressed");
    }
}
