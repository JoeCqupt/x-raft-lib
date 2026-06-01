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

/**
 * Public Raft API for the x-raft-lib core. Host applications wire
 * {@link io.github.xinfra.lab.raft.Storage} and
 * {@link io.github.xinfra.lab.raft.Transport} implementations into
 * {@link io.github.xinfra.lab.raft.Node} / {@link io.github.xinfra.lab.raft.RawNode}
 * via {@link io.github.xinfra.lab.raft.Config} and drive the
 * {@link io.github.xinfra.lab.raft.Ready} / advance loop.
 *
 * <h2>Nullness</h2>
 *
 * The public API package is {@link org.jspecify.annotations.NullMarked}: every
 * reference type is non-null by default. Anything that may legitimately be
 * absent is annotated {@link org.jspecify.annotations.Nullable} explicitly.
 * IDEs / NullAway / Checker Framework will use this to flag misuse.
 */
@org.jspecify.annotations.NullMarked
package io.github.xinfra.lab.raft;
