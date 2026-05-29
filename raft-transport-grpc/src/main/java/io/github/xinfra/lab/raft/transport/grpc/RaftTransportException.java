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
package io.github.xinfra.lab.raft.transport.grpc;

/**
 * Unchecked failure raised by the gRPC transport for unrecoverable
 * transport-level errors — most notably failing to bind the server socket in
 * {@link GrpcTransport#start()}.
 *
 * <p>Unchecked (rather than a checked {@code RaftException}) because the
 * {@link io.github.xinfra.lab.raft.Transport} SPI's lifecycle methods do not
 * declare checked exceptions, and a bind failure is a fatal startup condition:
 * the node cannot run, so callers should let it propagate and abort startup
 * rather than try to recover in place. Typed so hosts can distinguish a
 * transport setup failure from an arbitrary {@link RuntimeException}.
 */
public class RaftTransportException extends RuntimeException {

    public RaftTransportException(String message, Throwable cause) {
        super(message, cause);
    }

    public RaftTransportException(String message) {
        super(message);
    }
}
