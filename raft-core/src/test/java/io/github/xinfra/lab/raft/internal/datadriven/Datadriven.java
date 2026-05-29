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
package io.github.xinfra.lab.raft.internal.datadriven;
import io.github.xinfra.lab.raft.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Minimal datadriven test runner inspired by cockroachdb's datadriven, used
 * extensively in etcd-raft's interaction_test.go.
 *
 * <p>File format:
 * <pre>
 * # comment line
 * command-name arg1 key=value
 * arg-line-2
 * ----
 * expected
 * output
 * line(s)
 *
 * next-command ...
 * ----
 * expected output
 * </pre>
 *
 * <p>The {@code ----} separator splits a directive's input from its expected
 * output. Blank lines (or EOF) terminate a directive. Lines starting with
 * {@code #} are ignored.
 *
 * <p>This is intentionally a small subset of the etcd/cockroachdb format —
 * just enough to drive raft scenarios with a stable, diff-friendly format.
 */
public final class Datadriven {

    private Datadriven() {}

    /** A parsed directive: command + args + expected output. */
    public static final class Directive {
        public final String command;
        public final List<String> args;
        public final Map<String, String> kvArgs;
        public final String input;
        public final String expected;
        public final int lineNumber;

        Directive(String command, List<String> args, Map<String, String> kvArgs,
                  String input, String expected, int lineNumber) {
            this.command = command;
            this.args = args;
            this.kvArgs = kvArgs;
            this.input = input;
            this.expected = expected;
            this.lineNumber = lineNumber;
        }

        public String getKv(String key, String defaultValue) {
            return kvArgs.getOrDefault(key, defaultValue);
        }
    }

    /**
     * Runs {@code handler} against every directive in the file at {@code path}.
     * Asserts handler output matches the expected output of each directive.
     *
     * <p>Set the system property {@code datadriven.rewrite=true} to instead
     * <em>regenerate</em> the file's expected blocks from {@code handler}'s
     * actual output, then assert the rewrite happened (so the test fails and
     * the diff is obvious in CI). After review, commit the updated .txt file.
     * Mirrors etcd-raft's {@code go test -rewrite} workflow.
     *
     * @throws AssertionError if any directive's actual output disagrees with
     *         expected (in compare mode), or if the file was rewritten.
     */
    public static void run(Path path, Function<Directive, String> handler) throws IOException {
        boolean rewrite = Boolean.getBoolean("datadriven.rewrite");
        List<Directive> directives = parse(path);

        if (rewrite) {
            // Capture each directive's actual output and rewrite the entire file.
            List<String> actualOutputs = new ArrayList<>(directives.size());
            for (Directive d : directives) {
                try {
                    actualOutputs.add(normalize(handler.apply(d)));
                } catch (RuntimeException e) {
                    throw new AssertionError(
                            String.format("%s:%d: command '%s' threw %s during rewrite",
                                    path, d.lineNumber, d.command, e), e);
                }
            }
            rewriteFile(path, directives, actualOutputs);
            // Always fail with a clear message so the rewrite shows up in CI;
            // the user is expected to inspect the diff and re-run without the
            // rewrite flag.
            throw new AssertionError(String.format(
                    "%s rewritten with actual outputs (datadriven.rewrite=true). "
                            + "Inspect the diff and re-run without rewrite.", path));
        }

        for (Directive d : directives) {
            String actual;
            try {
                actual = handler.apply(d);
            } catch (RuntimeException e) {
                throw new AssertionError(
                        String.format("%s:%d: command '%s' threw %s",
                                path, d.lineNumber, d.command, e), e);
            }
            String normalizedActual = normalize(actual);
            String normalizedExpected = normalize(d.expected);
            if (!normalizedExpected.equals(normalizedActual)) {
                throw new AssertionError(String.format(
                        "%s:%d: directive '%s' output mismatch%n--- expected ---%n%s%n--- actual ---%n%s%n%n"
                                + "Re-run with -Ddatadriven.rewrite=true to regenerate.",
                        path, d.lineNumber, d.command,
                        normalizedExpected, normalizedActual));
            }
        }
    }

    /**
     * Reads the file fresh and rewrites every directive's expected block to
     * match {@code actualOutputs}. Preserves the original input lines and
     * leading comments / blank-line layout exactly.
     */
    private static void rewriteFile(Path path, List<Directive> directives, List<String> actualOutputs) throws IOException {
        List<String> lines = Files.readAllLines(path);
        StringBuilder out = new StringBuilder();
        int i = 0;
        int dIdx = 0;
        while (i < lines.size()) {
            // Pass through blanks/comments verbatim until next directive.
            while (i < lines.size()) {
                String l = lines.get(i).trim();
                if (l.isEmpty() || l.startsWith("#")) {
                    out.append(lines.get(i)).append('\n');
                    i++;
                } else break;
            }
            if (i >= lines.size()) break;
            // Directive input lines, up to "----".
            while (i < lines.size() && !lines.get(i).trim().equals("----")) {
                out.append(lines.get(i)).append('\n');
                i++;
            }
            if (i >= lines.size()) break;
            out.append("----").append('\n');
            i++;
            // Skip the old expected block.
            while (i < lines.size() && !lines.get(i).trim().isEmpty()) {
                i++;
            }
            // Write the new expected block.
            String newExpected = actualOutputs.get(dIdx++);
            if (!newExpected.isEmpty()) {
                out.append(newExpected).append('\n');
            }
        }
        Files.writeString(path, out.toString());
    }

    /** Strips trailing whitespace on each line and a trailing newline. */
    private static String normalize(String s) {
        if (s == null) return "";
        String[] lines = s.split("\n", -1);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < lines.length; i++) {
            String line = lines[i];
            // strip trailing whitespace
            int end = line.length();
            while (end > 0 && Character.isWhitespace(line.charAt(end - 1))) end--;
            sb.append(line, 0, end);
            if (i < lines.length - 1) sb.append('\n');
        }
        // trim trailing newlines
        while (sb.length() > 0 && sb.charAt(sb.length() - 1) == '\n') {
            sb.deleteCharAt(sb.length() - 1);
        }
        return sb.toString();
    }

    static List<Directive> parse(Path path) throws IOException {
        List<String> lines = Files.readAllLines(path);
        List<Directive> result = new ArrayList<>();
        int i = 0;
        while (i < lines.size()) {
            // skip blank lines and full-line comments
            while (i < lines.size()) {
                String l = lines.get(i).trim();
                if (l.isEmpty() || l.startsWith("#")) {
                    i++;
                } else {
                    break;
                }
            }
            if (i >= lines.size()) break;

            int directiveStart = i + 1; // 1-based line number
            // collect input lines until "----"
            List<String> inputLines = new ArrayList<>();
            while (i < lines.size() && !lines.get(i).trim().equals("----")) {
                inputLines.add(lines.get(i));
                i++;
            }
            if (i >= lines.size()) {
                throw new IOException(String.format(
                        "%s:%d: directive missing '----' separator",
                        path, directiveStart));
            }
            i++; // skip "----"

            // collect expected output until blank line or EOF
            StringBuilder expected = new StringBuilder();
            while (i < lines.size() && !lines.get(i).trim().isEmpty()) {
                if (expected.length() > 0) expected.append('\n');
                expected.append(lines.get(i));
                i++;
            }

            // parse first non-blank line as command, rest as input
            if (inputLines.isEmpty()) {
                throw new IOException(String.format(
                        "%s:%d: empty directive", path, directiveStart));
            }
            String firstLine = inputLines.get(0);
            ParsedCommand pc = parseCommandLine(firstLine);
            String input = inputLines.size() > 1
                    ? String.join("\n", inputLines.subList(1, inputLines.size()))
                    : "";
            result.add(new Directive(pc.command, pc.args, pc.kvArgs, input,
                    expected.toString(), directiveStart));
        }
        return result;
    }

    private static final class ParsedCommand {
        final String command;
        final List<String> args;
        final Map<String, String> kvArgs;
        ParsedCommand(String c, List<String> a, Map<String, String> kv) {
            command = c; args = a; kvArgs = kv;
        }
    }

    private static ParsedCommand parseCommandLine(String line) {
        // tokenize on whitespace; first token is the command
        String[] tokens = line.trim().split("\\s+");
        String cmd = tokens[0];
        List<String> args = new ArrayList<>();
        Map<String, String> kv = new LinkedHashMap<>();
        for (int j = 1; j < tokens.length; j++) {
            String t = tokens[j];
            int eq = t.indexOf('=');
            if (eq > 0) {
                kv.put(t.substring(0, eq), t.substring(eq + 1));
            } else {
                args.add(t);
            }
        }
        return new ParsedCommand(cmd, args, kv);
    }
}
