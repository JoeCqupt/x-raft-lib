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
import io.github.xinfra.lab.raft.*;

import io.github.xinfra.lab.raft.internal.datadriven.Datadriven;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Datadriven scenario tests, mirroring etcd-raft's TestInteraction. Each .txt
 * file under {@code src/test/resources/testdata/} is a scenario; one
 * {@link InteractionEnv} is created per file, and directives within the file
 * share that environment.
 *
 * <p>The framework is small (see {@link Datadriven}) but extensible: add new
 * commands to {@link InteractionEnv#handle} and write a new {@code .txt} file.
 * To regenerate expected outputs after an intentional change, prefer to inspect
 * the diff and edit the {@code .txt} file directly — the format is plain text.
 */
class InteractionTest {

    @TestFactory
    Stream<DynamicTest> scenarios() throws IOException {
        // Read scenarios directly from src/test/resources rather than via the
        // classloader (target/test-classes), so that -Ddatadriven.rewrite=true
        // updates the source file in place — exactly what the user wants to
        // commit. {@code user.dir} is the maven module root during test runs.
        Path testdataDir = Paths.get(System.getProperty("user.dir"),
                "src", "test", "resources", "testdata");
        if (!Files.isDirectory(testdataDir)) {
            return Stream.empty();
        }
        List<Path> files = new ArrayList<>();
        try (var paths = Files.list(testdataDir)) {
            paths.filter(p -> p.toString().endsWith(".txt"))
                    .sorted()
                    .forEach(files::add);
        }
        return files.stream().map(p -> DynamicTest.dynamicTest(p.getFileName().toString(),
                () -> {
                    InteractionEnv env = new InteractionEnv();
                    Datadriven.run(p, d -> {
                        try {
                            return env.handle(d);
                        } catch (io.github.xinfra.lab.raft.RaftException re) {
                            // Datadriven's handler is a Function (unchecked);
                            // wrap raft-layer rejections so the directive can
                            // still report the error in the .txt assertion.
                            throw new RuntimeException(re);
                        }
                    });
                }));
    }
}
