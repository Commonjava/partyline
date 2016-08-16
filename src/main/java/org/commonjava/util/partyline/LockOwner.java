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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.commons.lang.StringUtils.join;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.ref.WeakReference;

public class LockOwner
{

    private final WeakReference<Thread> threadRef;

    private final long threadId;

    private final String threadName;

    private final StackTraceElement[] lockOrigin;

    public LockOwner()
    {
        final Thread t = Thread.currentThread();
        this.threadRef = new WeakReference<Thread>( t );
        this.threadName = t.getName();
        this.threadId = t.getId();
        Logger logger = LoggerFactory.getLogger( getClass() );
        if ( logger.isDebugEnabled())
        {
            this.lockOrigin = t.getStackTrace();
        }
        else
        {
            this.lockOrigin = null;
        }
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
    public String toString()
    {
        return String.format( "LockOwner [%s(%s)]\n  %s", threadName, threadId,
                              lockOrigin == null ? "-suppressed-" : join( lockOrigin, "\n  " ) );
    }

}
