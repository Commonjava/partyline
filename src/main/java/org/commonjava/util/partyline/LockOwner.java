/**
 * Copyright (C) 2015 Red Hat, Inc. (jdcasey@commonjava.org)
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
package org.commonjava.util.partyline;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.ref.WeakReference;

public class LockOwner
    implements Closeable
{

    public enum LockType
    {
        INPUT, JOINABLE_OUTPUT, MANUAL;
    }

    private final WeakReference<Thread> threadRef;

    private final long threadId;

    private final String threadName;

    private final StackTraceElement[] lockOrigin;

    private InputStream inputStream;

    private OutputStream outputStream;

    private LockType lockType;

    public LockOwner( final OutputStream outputStream )
    {
        this.outputStream = outputStream;
        final Thread t = Thread.currentThread();
        this.threadRef = new WeakReference<Thread>( t );
        this.threadName = t.getName();
        this.threadId = t.getId();
        this.lockOrigin = t.getStackTrace();
        this.lockType = LockType.JOINABLE_OUTPUT;
    }

    public LockOwner( final InputStream inputStream )
    {
        this.inputStream = inputStream;
        final Thread t = Thread.currentThread();
        this.threadRef = new WeakReference<Thread>( t );
        this.threadName = t.getName();
        this.threadId = t.getId();
        this.lockOrigin = t.getStackTrace();
        this.lockType = LockType.INPUT;
    }

    public LockOwner()
    {
        this( (InputStream) null );
        this.lockType = LockType.MANUAL;
    }

    public boolean isAlive()
    {
        return threadRef.get() != null && threadRef.get()
                                                   .isAlive();
    }

    public long getThreadId()
    {
        return threadId;
    }

    public String getThreadName()
    {
        return threadName;
    }

    public StackTraceElement[] getLockOrigin()
    {
        return lockOrigin;
    }

    public Thread getThread()
    {
        return threadRef.get();
    }

    @Override
    public void close()
        throws IOException
    {
        if ( inputStream != null )
        {
            inputStream.close();
        }
        else if ( outputStream != null )
        {
            outputStream.close();
        }
    }

    public LockType getLockType()
    {
        return lockType;
    }

}
