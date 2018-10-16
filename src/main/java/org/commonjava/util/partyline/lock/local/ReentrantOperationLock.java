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
package org.commonjava.util.partyline.lock.local;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.commons.lang.StringUtils.join;

/**
 * Locks a single operation on a File in this FileTree, so competing operations ON THAT FILE have to wait, but
 * operations on other files can continue.
 */
public final class ReentrantOperationLock
{
    private ReentrantLock lock = new ReentrantLock();

    private Condition changed = lock.newCondition();

    private String locker;

    public boolean lock()
            throws InterruptedException
    {
        Logger logger = LoggerFactory.getLogger( getClass() );
        if ( logger.isTraceEnabled() )
        {
            logger.trace( "Locking: {} for: {}", this, Thread.currentThread().getName() );
//            logger.trace( "Locking: {} for: {} from:\n\n{}\n\n", this, Thread.currentThread().getName(),
//                          join( Thread.currentThread().getStackTrace(), "\n  " ) );
        }

        lock.lockInterruptibly();

        logger.trace( "Lock established." );
        return true;
    }

    public void unlock()
    {
        if ( lock.isHeldByCurrentThread() )
        {
            Logger logger = LoggerFactory.getLogger( getClass() );
            if ( logger.isTraceEnabled() )
            {
                logger.trace( "Locking: {} (locked by: {})", this, locker );
//                logger.trace( "Locking: {} (locked by: {}) from:\n\n{}\n\n", this, locker, join( Thread.currentThread().getStackTrace(), "\n  " ) );
            }

            changed.signal();
            lock.unlock();
            locker = null;

            logger.trace( "Locked released" );
        }
    }

    public void await( long timeoutMs )
            throws InterruptedException
    {
        if ( lock.isLocked() )
        {
            Logger logger = LoggerFactory.getLogger( getClass() );
            logger.trace( "Waiting for unlock of: {} by: {}", this, locker );
            changed.await( timeoutMs, TimeUnit.MILLISECONDS );
        }
    }

    public void signal()
    {
        if ( lock.isHeldByCurrentThread() )
        {
            Logger logger = LoggerFactory.getLogger( getClass() );
            logger.trace( "Signal from: {} in lock of: {} (locked by: {})", Thread.currentThread().getName(), this, locker );
            changed.signal();
        }
    }

    public String getLocker()
    {
        return locker;
    }
}
