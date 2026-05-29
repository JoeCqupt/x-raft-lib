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
package io.github.xinfra.lab.raft;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DefaultRaftLogger is a default implementation of the RaftLogger interface
 * backed by SLF4J. Corresponds to DefaultLogger in etcd-raft's logger.go.
 *
 * <p>Supports the custom {@code {:x}} placeholder for hex-formatting numeric
 * arguments, in addition to the standard SLF4J {@code {}} placeholder.
 * For example: {@code logger.info("{:x} became follower at term {}", nodeId, term)}
 * will format {@code nodeId} as a lowercase hexadecimal string.
 */
public class DefaultRaftLogger implements RaftLogger {
    private final Logger logger;

    public DefaultRaftLogger() {
        this.logger = LoggerFactory.getLogger("raft");
    }

    public DefaultRaftLogger(String name) {
        this.logger = LoggerFactory.getLogger(name);
    }

    public DefaultRaftLogger(Logger logger) {
        this.logger = logger;
    }

    @Override
    public void debug(String format, Object... args) {
        if (logger.isDebugEnabled()) {
            Object[] converted = convertHexArgs(format, args);
            logger.debug(normalizeFormat(format), converted);
        }
    }

    @Override
    public void info(String format, Object... args) {
        if (logger.isInfoEnabled()) {
            Object[] converted = convertHexArgs(format, args);
            logger.info(normalizeFormat(format), converted);
        }
    }

    @Override
    public void warn(String format, Object... args) {
        if (logger.isWarnEnabled()) {
            Object[] converted = convertHexArgs(format, args);
            logger.warn(normalizeFormat(format), converted);
        }
    }

    @Override
    public void error(String format, Object... args) {
        if (logger.isErrorEnabled()) {
            Object[] converted = convertHexArgs(format, args);
            logger.error(normalizeFormat(format), converted);
        }
    }

    @Override
    public void fatal(String format, Object... args) {
        Object[] converted = convertHexArgs(format, args);
        String normalized = normalizeFormat(format);
        logger.error(normalized, converted);
        throw new RaftInvariantException(formatMessage(normalized, converted));
    }

    @Override
    public void panic(String format, Object... args) {
        Object[] converted = convertHexArgs(format, args);
        String msg = formatMessage(normalizeFormat(format), converted);
        logger.error(msg);
        throw new RaftInvariantException(msg);
    }

    /**
     * Pre-processes arguments: for each {@code {:x}} placeholder in the format
     * string, converts the corresponding numeric argument to a hex string.
     * Standard {@code {}} placeholders are left as-is.
     */
    public static Object[] convertHexArgs(String format, Object[] args) {
        if (args == null || args.length == 0) {
            return args;
        }
        Object[] result = null; // lazy copy
        int argIdx = 0;
        int i = 0;
        while (i < format.length() - 1 && argIdx < args.length) {
            if (format.charAt(i) == '{') {
                if (i + 3 < format.length() && format.charAt(i + 1) == ':' && format.charAt(i + 2) == 'x' && format.charAt(i + 3) == '}') {
                    // {:x} placeholder — convert arg to hex
                    if (result == null) {
                        result = args.clone();
                    }
                    result[argIdx] = toHex(args[argIdx]);
                    argIdx++;
                    i += 4;
                } else if (format.charAt(i + 1) == '}') {
                    // {} placeholder — keep as-is
                    argIdx++;
                    i += 2;
                } else {
                    i++;
                }
            } else {
                i++;
            }
        }
        return result != null ? result : args;
    }

    /**
     * Replaces all {@code {:x}} with {@code {}} so SLF4J can handle the format.
     */
    public static String normalizeFormat(String format) {
        // Fast path: no {:x} in the format
        if (format.indexOf("{:x}") < 0) {
            return format;
        }
        return format.replace("{:x}", "{}");
    }

    private static Object toHex(Object arg) {
        if (arg instanceof Long l) {
            return Long.toHexString(l);
        } else if (arg instanceof Integer n) {
            return Integer.toHexString(n);
        } else if (arg instanceof Number n) {
            return Long.toHexString(n.longValue());
        }
        return arg;
    }

    private String formatMessage(String format, Object... args) {
        // Simple {} replacement for format strings
        String result = format;
        for (Object arg : args) {
            int idx = result.indexOf("{}");
            if (idx >= 0) {
                result = result.substring(0, idx) + arg + result.substring(idx + 2);
            }
        }
        return result;
    }

    /** Global default logger instance. */
    private static volatile RaftLogger defaultLogger = new DefaultRaftLogger();

    public static RaftLogger getDefault() {
        return defaultLogger;
    }

    public static void setDefault(RaftLogger logger) {
        defaultLogger = logger;
    }
}
