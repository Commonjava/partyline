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

import org.commonjava.util.partyline.spi.JoinableFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LocalLockManager
{
    private final Map<String, ReentrantOperationLock> operationLocks = new ConcurrentHashMap<>();

    private final Logger logger = LoggerFactory.getLogger( getClass() );

    /**
     * Use a {@link java.util.concurrent.locks.ReentrantLock} keyed to the absolute path of the specified file to ensure
     * only one operation at a time manipulates the accounting information associated with the file.
     *
     * This method synchronizes on the operationLocks map in order to retrieve / create the ReentrantLock lazily. Once
     * created, this ReentrantLock also gets propagated into the {@link JoinableFile} instance created for the file.
     *
     * Using ReentrantLock per path avoids the need to hold a lock on the whole tree every time we need to initialize
     * the instances related to a new file. Instead, we take a short lock on operationLocks to get the ReentrantLock,
     * then use the ReentrantLock for the longer operations required to initialize a file, open a stream, delete a file,
     * close a file, etc.
     *
     * @param path The file that is the subject of the operation we want to execute
     * @param op The operation to execute, once we've locked the ReentrantLock associated with the file
     * @param <T> The result type of the specified operation
     * @return the result of the specified operation
     * @throws IOException
     * @throws InterruptedException
     */
    public <T> T reentrantSynchronous( String path, ReentrantOperation<T> op ) throws IOException, InterruptedException
    {
        ReentrantOperationLock opLock = null;

        try
        {
            synchronized ( OPERATION_LOCKS_MUTEX )
            {
                opLock = operationLocks.computeIfAbsent( path, k -> {
                    ReentrantOperationLock lock = new ReentrantOperationLock();
                    logger.trace( "Initializing new opLock: {} for path: {}", lock, path );
                    return lock;
                } );
                logger.trace( "Using opLock: {} for path: {}", opLock, path );

                if ( !opLock.lock() )
                {
                    throw new IOException(
                                    "Failed to lock for: " + path + ", opLock: " + opLock + " (currently locked by: "
                                                    + opLock.getLocker() + ")" );
                }
            }

            logger.trace( "Locked ReentrantSynchronousOperation: {} for path: {}. Proceeding with file operation.",
                          opLock, path );

            return op.execute( opLock );
        }
        finally
        {
            if ( opLock != null )
            {
                try
                {
                    opLock.unlock();
                }
                catch ( Throwable t )
                {
                    logger.error( "Failed to unlock: " + path, t );
                }
            }
        }
    }

    /**
     * Although operationLocks is a concurrent map, the computeIfAbsent only promise the atomicity of the lock-creating function.
     * We still need a mutex to sync the adding/removing of locks.
     */
    private final Object OPERATION_LOCKS_MUTEX = new Object();

    /**
     * This is invoked when the FileTree removes a fileEntry from entryMap. In this case, no one is using the lock anymore.
     * @param path
     * @param opLock
     */
    public void removeReentrantLock( String path, ReentrantOperationLock opLock )
    {
        synchronized ( OPERATION_LOCKS_MUTEX )
        {
            if ( !opLock.isLocked() )
            {
                logger.trace( "Remove lock {} for path {}", opLock, path );
                operationLocks.remove( path );
            }
        }
    }
}
