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

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Locks a single operation on a File in this FileTree, so competing operations ON THAT FILE have to wait, but
 * operations on other files can continue.
 */
final class FileOperationLock
{
    private ReentrantLock lock = new ReentrantLock();

    private Condition changed = lock.newCondition();

    public boolean lock()
    {
        return lock.tryLock();
    }

    public void unlock()
    {
        if ( lock.isLocked() )
        {
            lock.unlock();
        }
    }

    public void await( long timeoutMs )
            throws InterruptedException
    {
        changed.await( timeoutMs, TimeUnit.MILLISECONDS );
    }

    public void signal()
    {
        changed.signal();
    }

    public <T> T lockAnd( long timeout, TimeUnit unit, LockedFileOperation<T> op )
            throws IOException, InterruptedException
    {
        long end = timeout < 1 ? -1 : System.currentTimeMillis() + TimeUnit.MILLISECONDS.convert( timeout, unit );
        boolean locked = false;
        try
        {
            while ( end < 1 || System.currentTimeMillis() < end )
            {
                locked = lock();
                if ( locked )
                {
                    return op.execute( this );
                }
            }

            return null;
        }
        finally
        {
            if ( locked ) unlock();
        }
    }

}
