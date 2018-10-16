/**
 * Copyright (C) 2015 Red Hat, Inc. (nos-devel@redhat.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.commonjava.util.partyline.spi;

import org.commonjava.util.partyline.lock.local.LocalLockOwner;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface JoinableFile
        extends AutoCloseable, Closeable
{
    static String labelFor( boolean doOutput, String threadName )
    {
        return (doOutput ? "WRITE via " : "READ via ") + threadName;
    }

    LocalLockOwner getLockOwner();

    // only public for testing purposes...
    abstract OutputStream getOutputStream();

    boolean isJoinable();

    boolean isDirectory();

    InputStream joinStream()
            throws IOException, InterruptedException;

    boolean isWriteLocked();

    /**
     * Mark this stream as closed. Don't close the underlying channel if
     * there are still open input streams...allow their close methods to trigger that if the ref count drops
     * to 0.
     */
    @Override
    void close()
            throws IOException;

    String getPath();

    boolean isOpen();

    String reportOwnership();
}
