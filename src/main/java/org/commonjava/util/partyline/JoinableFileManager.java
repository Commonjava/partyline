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
/*******************************************************************************
* Copyright (c) 2015 Red Hat, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the GNU Public License v3.0
* which accompanies this distribution, and is available at
* http://www.gnu.org/licenses/gpl.html
*
* Contributors:
* Red Hat, Inc. - initial API and implementation
******************************************************************************/
package org.commonjava.util.partyline;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.commonjava.util.partyline.callback.AbstractStreamCallbacks;
import org.commonjava.util.partyline.callback.CallbackInputStream;
import org.commonjava.util.partyline.callback.CallbackOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * File manager that attempts to manage read/write locks in the presence of output streams that will allow simultaneous access to read the content
 * they are writing. Also allows the user to lock/unlock files manually in case they need to be used outside the normal streaming use cases.
 * 
 * @author jdcasey
 */
public class JoinableFileManager
{

    private final Logger logger = LoggerFactory.getLogger( getClass() );

    private final Map<File, JoinableOutputStream> joinableStreams = new HashMap<>();

    private final Map<File, WeakReference<Thread>> activeFiles = new HashMap<>();

    private final Set<File> manualLocks = new HashSet<>();

    /**
     * If the file isn't marked as active, create a new {@link JoinableOutputStream} to the specified file and pass it back to the user.
     */
    public OutputStream openOutputStream( final File file )
        throws IOException
    {
        return openOutputStream( file, -1 );
    }

    /**
     * If the file isn't marked as active, create a new {@link JoinableOutputStream} to the specified file and pass it back to the user. If the file is locked, wait for the specified milliseconds before giving up.
     */
    public OutputStream openOutputStream( final File file, final long timeout )
        throws IOException
    {
        logger.debug( ">>>OPEN OUTPUT: {}, timeout: {}", file, timeout );
        synchronized ( activeFiles )
        {
            final boolean proceed = timeout > 0 ? waitForFile( file, timeout ) : waitForFile( file );
            if ( !proceed )
            {
                logger.debug( "<<<OPEN OUTPUT (timeout)" );
                return null;
            }

            final JoinableOutputStream out = new JoinableOutputStream( file );
            joinableStreams.put( file, out );

            logger.debug( "Locked by: {}", Thread.currentThread()
                                                 .getName() );
            activeFiles.put( file, new WeakReference<Thread>( Thread.currentThread() ) );
            activeFiles.notifyAll();

            logger.debug( "<<<OPEN OUTPUT" );
            return new CallbackOutputStream( out, new StreamCallback( file, out ) );
        }
    }

    /**
     * If there is an active {@link JoinableOutputStream}, call {@link JoinableOutputStream#joinStream()} and return it to the user. Otherwise, open
     * a new {@link FileInputStream} to the specified file, wrap it in a {@link CallbackInputStream} to notify this manager when it closes, and pass
     * the result back to the user.
     */
    public InputStream openInputStream( final File file )
        throws FileNotFoundException, IOException
    {
        return openInputStream( file, -1 );
    }

    /**
     * If there is an active {@link JoinableOutputStream}, call {@link JoinableOutputStream#joinStream()} and return it to the user. Otherwise, open
     * a new {@link FileInputStream} to the specified file, wrap it in a {@link CallbackInputStream} to notify this manager when it closes, and pass
     * the result back to the user. If the file is locked for reads, wait for the specified milliseconds before giving up.
     */
    public InputStream openInputStream( final File file, final long timeout )
        throws FileNotFoundException, IOException
    {
        synchronized ( activeFiles )
        {
            logger.debug( ">>>OPEN INPUT: {}, timeout: {}", file, timeout );
            final JoinableOutputStream joinable = joinableStreams.get( file );
            if ( joinable != null )
            {
                logger.debug( "<<<OPEN INPUT (joined)" );
                return joinable.joinStream();
            }
            else
            {
                final boolean proceed = timeout > 0 ? waitForFile( file, timeout ) : waitForFile( file );
                if ( !proceed )
                {
                    logger.debug( "<<<OPEN INPUT (timeout)" );
                    return null;
                }

                logger.debug( "Locked by: {}", Thread.currentThread()
                                                     .getName() );
                activeFiles.put( file, new WeakReference<Thread>( Thread.currentThread() ) );
                activeFiles.notifyAll();

                logger.debug( "<<<OPEN INPUT (raw), called from:\n  {}", "disabled stacktrace" /*StringUtils.join( truncatedStackTrace(), "\n  " )*/);
                return new CallbackInputStream( new FileInputStream( file ), new StreamCallback( file ) );
            }
        }
    }

    /**
     * Manually lock the specified file to prevent opening any streams via this manager (until manually unlocked).
     */
    public boolean lock( final File file )
    {
        logger.debug( ">>>LOCK: {}", file );
        synchronized ( activeFiles )
        {
            if ( getActiveThread( file ) != null )
            {
                logger.debug( "<<<LOCK (failed)" );
                return false;
            }

            logger.debug( "Locked by: {}", Thread.currentThread()
                                                 .getName() );
            activeFiles.put( file, new WeakReference<Thread>( Thread.currentThread() ) );
            activeFiles.notifyAll();

            manualLocks.add( file );
        }

        logger.debug( "<<<LOCK (success)" );
        return true;
    }

    /**
     * If the specified file was manually locked, unlock it and return true. Otherwise, return false.
     */
    public boolean unlock( final File file )
    {
        // TODO: Verify the thread is the same?
        logger.debug( ">>>LOCK: {}", file );
        synchronized ( activeFiles )
        {
            manualLocks.remove( file );
            final WeakReference<Thread> ref = activeFiles.remove( file );
            final Thread t = ref == null ? null : ref.get();

            logger.debug( "Unlocked by: {}. Previously locked by: {}", Thread.currentThread()
                                                                             .getName(),
                          t == null ? "-NONE-" : t.getName() );

        }

        logger.debug( "<<<UNLOCK (success)" );
        return true;
    }

    /**
     * Check if a particular file has been locked externally (manually).
     */
    public boolean isManuallyLocked( final File file )
    {
        return manualLocks.contains( file );
    }

    /**
     * Check if the specified file is locked against write operations. Files are write-locked if any other file access is active.
     */
    public boolean isWriteLocked( final File file )
    {
        return getActiveThread( file ) != null;
    }

    /**
     * Check if the specified file is locked against read operations. This is only the case if the file is active AND not in use via a 
     * {@link JoinableOutputStream}, since joinable streams allow multiple reads while the write operation is ongoing.
     */
    public boolean isReadLocked( final File file )
    {
        return getActiveThread( file ) != null && !joinableStreams.containsKey( file );
    }

    /**
     * Wait the specified timeout milliseconds for write access on the specified file to become available. Return false if the timeout elapses without
     * the file becoming available for writes.
     * 
     * @see #isWriteLocked(File)
     */
    public boolean waitForWriteUnlock( final File file, final long timeout )
    {
        logger.debug( ">>>WAIT (write): {}, timeout: {}", file, timeout );
        synchronized ( activeFiles )
        {
            return waitForFile( file, timeout );
        }
    }

    /**
     * Wait the specified timeout milliseconds for write access on the specified file to become available. Return false if the timeout elapses without
     * the file becoming available for writes.
     * 
     * @see #isWriteLocked(File)
     */
    public boolean waitForWriteUnlock( final File file )
    {
        logger.debug( ">>>WAIT (write): {}, timeout: {}", file, -1 );
        synchronized ( activeFiles )
        {
            return waitForFile( file );
        }
    }

    /**
     * Wait the specified timeout milliseconds for read access on the specified file to become available. Return false if the timeout elapses without
     * the file becoming available for reads. If a {@link JoinableOutputStream} is available for the file, don't wait (immediately return true).
     * 
     * @see #isReadLocked(File)
     */
    public boolean waitForReadUnlock( final File file, final long timeout )
    {
        logger.debug( ">>>WAIT (read): {}, timeout: {}", file, timeout );
        if ( joinableStreams.containsKey( file ) )
        {
            logger.debug( "<<<WAIT (read)" );
            return true;
        }

        synchronized ( activeFiles )
        {
            return waitForFile( file, timeout );
        }
    }

    /**
     * Wait the specified timeout milliseconds for read access on the specified file to become available. Return false if the timeout elapses without
     * the file becoming available for reads. If a {@link JoinableOutputStream} is available for the file, don't wait (immediately return true).
     * 
     * @see #isReadLocked(File)
     */
    public boolean waitForReadUnlock( final File file )
    {
        logger.debug( ">>>WAIT (read): {}, timeout: {}", file, -1 );
        if ( joinableStreams.containsKey( file ) )
        {
            logger.debug( "<<<WAIT (read)" );
            return true;
        }

        synchronized ( activeFiles )
        {
            return waitForFile( file );
        }
    }

    private boolean waitForFile( final File file )
    {

        return waitForFile( file, -1 );
    }

    private boolean waitForFile( final File file, final long timeout )
    {
        logger.debug( ">>>WAIT (any file activity): {}, timeout: {}", file, timeout );
        if ( getActiveThread( file ) == null )
        {
            logger.debug( "<<<WAIT (any file activity): Not locked" );
            return true;
        }

        logger.debug( "wait called from:\n  {}", "disabled stacktrace" /*StringUtils.join( truncatedStackTrace(), "\n  " )*/);

        boolean proceed = false;
        //        System.out.println( "Waiting (" + ( timeout < 0 ? "indeterminate time" : timeout + "ms" ) + ") for: " + file );

        final long ends = timeout < 0 ? -1 : System.currentTimeMillis() + timeout;
        while ( ends < 0 || System.currentTimeMillis() < ends )
        {
            final Thread thread = getActiveThread( file );
            if ( thread == null )
            {
                logger.debug( "Lock cleared for: " + file );
                proceed = true;
                break;
            }
            //            else
            //            {
            //                logger.debug( "Waiting for thread: {} to release lock on: {}", thread.getName(), file );
            //            }

            try
            {
                activeFiles.wait( ( timeout > 0 && timeout < 1000 ) ? timeout : 1000 );
            }
            catch ( final InterruptedException e )
            {
                Thread.currentThread()
                      .interrupt();
                proceed = false;
            }
        }

        logger.debug( "<<<WAIT (any file activity): {}", proceed );
        return proceed;
    }

    //    private StackTraceElement[] truncatedStackTrace()
    //    {
    //        final StackTraceElement[] stackTrace = Thread.currentThread()
    //                                                     .getStackTrace();
    //
    //        final int offset = 2;
    //        final int desiredSize = 8;
    //        final int size = stackTrace.length > desiredSize + offset ? desiredSize : stackTrace.length - offset;
    //
    //        final StackTraceElement[] elts = new StackTraceElement[size];
    //        System.arraycopy( stackTrace, offset, elts, 0, size );
    //
    //        return elts;
    //    }

    private Thread getActiveThread( final File file )
    {
        synchronized ( activeFiles )
        {
            final WeakReference<Thread> ref = activeFiles.get( file );
            return ref == null ? null : ref.get();
        }
    }

    private final class StreamCallback
        extends AbstractStreamCallbacks
    {
        private final Logger logger = LoggerFactory.getLogger( getClass() );

        private final File file;

        private JoinableOutputStream stream;

        StreamCallback( final File file, final JoinableOutputStream stream )
        {
            this.file = file;
            this.stream = stream;
        }

        StreamCallback( final File file )
        {
            this.file = file;
        }

        @Override
        public void closed()
        {
            logger.debug( ">>>CLOSE/UNLOCK: {}", file );
            synchronized ( activeFiles )
            {
                final Thread t = getActiveThread( file );
                if ( t != null )
                {
                    logger.debug( "Unlocking. Previously locked by: {}", Thread.currentThread()
                                                                               .getName() );

                    activeFiles.remove( file );
                    if ( stream != null )
                    {
                        joinableStreams.remove( file );
                    }

                    activeFiles.notifyAll();
                }
            }
            logger.debug( "<<<CLOSE/UNLOCK" );
        }
    }
}
