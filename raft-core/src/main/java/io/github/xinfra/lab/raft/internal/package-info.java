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
 * Internal implementation of the Raft state machine. <b>Not public API.</b>
 *
 * <p>Everything under {@code io.github.xinfra.lab.raft.internal} (including the
 * {@code confchange}, {@code quorum}, and {@code tracker} subpackages) is an
 * implementation detail. Types here may be added, removed, renamed, or have
 * their signatures changed in any release without notice, even though Java
 * forces some of them to be declared {@code public} so the API classes in
 * {@code io.github.xinfra.lab.raft} can reach them across the package boundary.
 *
 * <p>Application code should depend only on the public API package
 * {@code io.github.xinfra.lab.raft} ({@link io.github.xinfra.lab.raft.Node},
 * {@link io.github.xinfra.lab.raft.RawNode}, {@link io.github.xinfra.lab.raft.Config},
 * {@link io.github.xinfra.lab.raft.Ready}, {@link io.github.xinfra.lab.raft.Storage},
 * {@link io.github.xinfra.lab.raft.Transport}, and friends).
 *
 * <p>{@link io.github.xinfra.lab.raft.Status} no longer leaks {@code tracker} /
 * {@code quorum} types: it exposes the dedicated public view types
 * {@link io.github.xinfra.lab.raft.PeerProgress},
 * {@link io.github.xinfra.lab.raft.TrackerConfig}, and
 * {@link io.github.xinfra.lab.raft.ProgressState} instead.
 */
package io.github.xinfra.lab.raft.internal;
