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
package io.github.xinfra.lab.raft.internal.tracker;
import io.github.xinfra.lab.raft.*;
import io.github.xinfra.lab.raft.internal.*;

/**
 * Inflights limits the number of MsgApp sent to followers but not yet acknowledged.
 * It uses a ring buffer internally.
 */
public class Inflights {
    private int start;
    private int count;
    private long bytes;
    private final int size;
    private final long maxBytes;
    private long[] indexBuffer;
    private long[] bytesBuffer;

    public Inflights(int size, long maxBytes) {
        this.size = size;
        this.maxBytes = maxBytes;
        this.indexBuffer = new long[0];
        this.bytesBuffer = new long[0];
    }

    @Override
    public Inflights clone() {
        Inflights ins = new Inflights(size, maxBytes);
        ins.start = this.start;
        ins.count = this.count;
        ins.bytes = this.bytes;
        ins.indexBuffer = this.indexBuffer.clone();
        ins.bytesBuffer = this.bytesBuffer.clone();
        return ins;
    }

    public void add(long index, long bytes) {
        if (full()) {
            throw new IllegalStateException("cannot add into a Full inflights");
        }
        int next = start + count;
        if (next >= size) {
            next -= size;
        }
        if (next >= indexBuffer.length) {
            grow();
        }
        indexBuffer[next] = index;
        bytesBuffer[next] = bytes;
        count++;
        this.bytes += bytes;
    }

    private void grow() {
        int newSize = indexBuffer.length * 2;
        if (newSize == 0) {
            newSize = 1;
        } else if (newSize > size) {
            newSize = size;
        }
        long[] newIndexBuffer = new long[newSize];
        long[] newBytesBuffer = new long[newSize];
        System.arraycopy(indexBuffer, 0, newIndexBuffer, 0, indexBuffer.length);
        System.arraycopy(bytesBuffer, 0, newBytesBuffer, 0, bytesBuffer.length);
        indexBuffer = newIndexBuffer;
        bytesBuffer = newBytesBuffer;
    }

    public void freeLE(long to) {
        if (count == 0 || to < indexBuffer[start]) {
            return;
        }

        int idx = start;
        int i;
        long freedBytes = 0;
        for (i = 0; i < count; i++) {
            if (to < indexBuffer[idx]) {
                break;
            }
            freedBytes += bytesBuffer[idx];
            idx++;
            if (idx >= size) {
                idx -= size;
            }
        }
        count -= i;
        bytes -= freedBytes;
        start = idx;
        if (count == 0) {
            start = 0;
        }
    }

    public boolean full() {
        return count == size || (maxBytes != 0 && bytes >= maxBytes);
    }

    public int count() {
        return count;
    }

    public void reset() {
        start = 0;
        count = 0;
        bytes = 0;
    }
}
