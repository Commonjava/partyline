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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.commonjava.util.partyline.callback.AbstractStreamCallbacks;
import org.commonjava.util.partyline.callback.CallbackInputStream;
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

    private final Map<File, JoinableOutputStream> joinableStreams = new ConcurrentHashMap<>();

    private final Map<File, LockOwner> activeFiles = new ConcurrentHashMap<>();

    private final Set<File> manualLocks = new HashSet<>();

    private final Timer timer;

    private ReportingTask reporter;

    public JoinableFileManager()
    {
        this.timer = new Timer( true );
    }

    public void cleanupCurrentThread()
    {
        final long id = Thread.currentThread().getId();
        for ( final File f : activeFiles.keySet() )
        {
            final LockOwner owner = activeFiles.get( f );
            if ( owner != null && owner.getThreadId() == id )
            {
                synchronized ( activeFiles )
                {
                    activeFiles.remove( f );
                    activeFiles.notifyAll();
                }

                final StringBuilder sb = new StringBuilder();
                sb.append( "CLEARING ORPHANED LOCK:\nFile: " )
                  .append( f )
                  .append( "\nOwned by thread: " )
                  .append( owner.getThreadName() )
                  .append( " (ID: " )
                  .append( owner.getThreadId() )
                  .append( ")" )
                  .append( "\nLock type: " )
                  .append( owner.getLockType() )
                  .append( "\nLocked at:\n" );

                for ( final StackTraceElement elt : owner.getLockOrigin() )
                {
                    sb.append( "\n  " ).append( elt );
                }

                sb.append( "\n\n" );

                logger.error( sb.toString() );

                IOUtils.closeQuietly( owner );
            }
        }

    }

    public synchronized void startReporting()
    {
        startReporting( 0, 10000 );
    }

    public synchronized void startReporting( final long delay, final long period )
    {
        if ( reporter == null )
        {
            logger.info( "Starting file-lock statistics reporting with initial delay: {}ms and period: {}ms", delay,
                         period );
            reporter = new ReportingTask();
            timer.schedule( reporter, delay, period );
        }
    }

    public synchronized void stopReporting()
    {
        if ( reporter != null )
        {
            logger.info( "Stopping file-lock statistics reporting." );
            reporter.cancel();
        }
    }

    public Map<File, CharSequence> getActiveLocks()
    {
        final Map<File, CharSequence> active = new HashMap<File, CharSequence>();

        Set<File> afs;
        synchronized ( activeFiles )
        {
            afs = new HashSet<>( activeFiles.keySet() );
        }

        for ( final File f : afs )
        {
            final StringBuilder owner = new StringBuilder();

            final LockOwner ref = activeFiles.get( f );

            if ( ref == null )
            {
                owner.append( "UNKNOWN OWNER; REF IS NULL." );
            }
            else
            {
                final Thread t = ref.getThread();

                if ( t == null )
                {
                    owner.append( "UNKNOWN OWNER; REF IS EMPTY." );
                }
                else
                {
                    owner.append( t.getName() );
                    if ( !t.isAlive() )
                    {
                        owner.append( " (DEAD)" );
                    }
                }
            }

            if ( manualLocks.contains( f ) )
            {
                owner.append( " (MANUALLY LOCKED)" );
            }
            else if ( joinableStreams.containsKey( f ) )
            {
                owner.append( " (JOINABLE WRITE)" );
            }
            else
            {
                owner.append( " (MANAGED READ)" );
            }

            active.put( f, owner );
        }

        return active;
    }

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
        logger.trace( ">>>OPEN OUTPUT: {} with timeout: {}", file, timeout );
        synchronized ( activeFiles )
        {
            final boolean proceed = timeout > 0 ? waitForFile( file, timeout ) : waitForFile( file );
            if ( !proceed )
            {
                logger.trace( "<<<OPEN OUTPUT (timeout)" );
                return null;
            }

            // FIXME: Nested synchronization blocks, could create deadlock!
            final JoinableOutputStream out = new JoinableOutputStream( file, new StreamCallback( file, true ) );
            synchronized ( joinableStreams )
            {
                joinableStreams.put( file, out );
            }

            logger.debug( "Locked by: {}", Thread.currentThread().getName() );
            activeFiles.put( file, new LockOwner( out ) );
            activeFiles.notifyAll();

            logger.trace( "<<<OPEN OUTPUT" );
            return out;
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
            logger.trace( ">>>OPEN INPUT: {} with timeout: {}", file, timeout );

            // FIXME: Nested synchronization blocks, could create deadlock!
            final JoinableOutputStream joinable;
            synchronized ( joinableStreams )
            {
                joinable = joinableStreams.get( file );
            }

            if ( joinable != null )
            {
                logger.trace( "<<<OPEN INPUT (joined)" );
                return joinable.joinStream();
            }
            else
            {
                final boolean proceed = timeout > 0 ? waitForFile( file, timeout ) : waitForFile( file );
                if ( !proceed )
                {
                    logger.trace( "<<<OPEN INPUT (timeout)" );
                    return null;
                }

                logger.debug( "Locked by: {}", Thread.currentThread().getName() );

                final InputStream in = new FileInputStream( file );
                activeFiles.put( file, new LockOwner( in ) );
                activeFiles.notifyAll();

                logger.trace( "<<<OPEN INPUT (raw), called from:\n  {}", stackTrace() );
                return new CallbackInputStream( in, new StreamCallback( file ) );
            }
        }
    }

    /**
     * Manually lock the specified file to prevent opening any streams via this manager (until manually unlocked).
     */
    public boolean lock( final File file )
    {
        logger.trace( ">>>MANUAL LOCK: {} at:\n  {}", file, stackTrace() );
        synchronized ( activeFiles )
        {
            final LockOwner ref = activeFiles.get( file );
            if ( ref != null && ref.isAlive() )
            {
                logger.trace( "<<<MANUAL LOCK (failed)" );
                return false;
            }

            if ( ref != null )
            {
                // if we get here, the ref is non-null but dead. Clear it.
                IOUtils.closeQuietly( ref );
            }

            logger.debug( "Locked by: {}", Thread.currentThread().getName() );

            activeFiles.put( file, new LockOwner() );
            activeFiles.notifyAll();

            manualLocks.add( file );
        }

        logger.trace( "<<<MANUAL LOCK (success)" );
        return true;
    }

    /**
     * If the specified file was manually locked, unlock it and return true. Otherwise, return false.
     */
    public boolean unlock( final File file )
    {
        logger.trace( ">>>MANUAL UNLOCK: {} at:\n  {}", file, stackTrace() );
        synchronized ( activeFiles )
        {
            // TODO: atomic, no sync needed?
            if ( joinableStreams.containsKey( file ) )
            {
                logger.warn( "Manual unlock called for file with active joinable output stream: {}", file );
            }

            manualLocks.remove( file );
            final LockOwner ref = activeFiles.remove( file );
            if ( ref == null )
            {
                logger.trace( "<<<MANUAL UNLOCK (not locked)" );
                return true;
            }
            else if ( !ref.isAlive() )
            {
                IOUtils.closeQuietly( ref );
                logger.trace( "<<<MANUAL UNLOCK (lock orphaned)" );
            }
            else if ( ref.getThreadId() != Thread.currentThread().getId() )
            {
                logger.warn( "Unlock attempt on file: {} by different thread!\n  locker: {}\n  unlocker: {})", file,
                             ref.getThreadName(), Thread.currentThread().getName() );

                logger.trace( "<<<MANUAL LOCK (allowed unlock attempt by different thread! locker: {}, unlocker: {})",
                              ref.getThreadName(), Thread.currentThread().getName() );
            }

            IOUtils.closeQuietly( ref );
            activeFiles.notifyAll();
            logger.debug( "Unlocked by: {}. Previously locked by: {}", Thread.currentThread().getName(),
                          ref.getThreadName() );

        }

        logger.trace( "<<<MANUAL UNLOCK (success)" );
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
        final LockOwner ref = activeFiles.get( file );
        return ref != null && ref.isAlive();
    }

    /**
     * Check if the specified file is locked against read operations. This is only the case if the file is active AND not in use via a 
     * {@link JoinableOutputStream}, since joinable streams allow multiple reads while the write operation is ongoing.
     */
    public boolean isReadLocked( final File file )
    {
        final LockOwner ref = activeFiles.get( file );
        // TODO: atomic, no sync needed?
        return ref != null && ref.isAlive() && !joinableStreams.containsKey( file );
    }

    /**
     * Wait the specified timeout milliseconds for write access on the specified file to become available. Return false if the timeout elapses without
     * the file becoming available for writes.
     *
     * @see #isWriteLocked(File)
     */
    public boolean waitForWriteUnlock( final File file, final long timeout )
    {
        logger.trace( ">>>WAIT (write): {} with timeout: {}", file, timeout );
        synchronized ( activeFiles )
        {
            try
            {
                return waitForFile( file, timeout );
            }
            finally
            {
                logger.trace( "<<<WAIT (write): {}, timeout: {}", file, timeout );
            }
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
        logger.trace( ">>>WAIT (write): {} with timeout: {}", file, -1 );
        synchronized ( activeFiles )
        {
            try
            {
                return waitForFile( file );
            }
            finally
            {
                logger.trace( "<<<WAIT (write): {}, timeout: {}", file, -1 );
            }
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
        logger.trace( ">>>WAIT (read): {} with timeout: {}", file, timeout );
        // TODO: atomic, no sync needed?
        if ( joinableStreams.containsKey( file ) )
        {
            logger.trace( "<<<WAIT (read)" );
            return true;
        }

        synchronized ( activeFiles )
        {
            try
            {
                return waitForFile( file, timeout );
            }
            finally
            {
                logger.trace( "<<<WAIT (read): {}, timeout: {}", file, timeout );
            }
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
        logger.trace( ">>>WAIT (read): {} with timeout: {}", file, -1 );
        // TODO: atomic, no sync needed?
        if ( joinableStreams.containsKey( file ) )
        {
            logger.trace( "<<<WAIT (read)" );
            return true;
        }

        synchronized ( activeFiles )
        {
            try
            {
                return waitForFile( file );
            }
            finally
            {
                logger.trace( "<<<WAIT (read): {}, timeout: {}", file, -1 );
            }
        }
    }

    private boolean waitForFile( final File file )
    {

        return waitForFile( file, -1 );
    }

    private boolean waitForFile( final File file, final long timeout )
    {
        logger.trace( ">>>WAIT (any file activity): {} with timeout: {}", file, timeout );
        LockOwner ref = activeFiles.get( file );
        if ( ref == null )
        {
            logger.trace( "<<<WAIT (any file activity): Not locked" );
            return true;
        }
        else if ( !ref.isAlive() )
        {
            activeFiles.remove( file );
            IOUtils.closeQuietly( ref );
            logger.trace( "<<<WAIT (any file activity): Not locked" );
            return true;
        }

        logger.debug( "wait called from:\n  {}", "disabled stacktrace" /*StringUtils.join( truncatedStackTrace(), "\n  " )*/ );

        boolean proceed = false;
        //        System.out.println( "Waiting (" + ( timeout < 0 ? "indeterminate time" : timeout + "ms" ) + ") for: " + file );

        final long ends = timeout < 0 ? -1 : System.currentTimeMillis() + timeout;
        while ( ends < 0 || System.currentTimeMillis() < ends )
        {
            ref = activeFiles.get( file );
            if ( ref == null )
            {
                logger.debug( "Lock cleared for: {}", file );
                proceed = true;
                break;
            }
            else if ( !ref.isAlive() )
            {
                activeFiles.remove( file );
                IOUtils.closeQuietly( ref );
                logger.debug( "(Orphaned) Lock cleared for: {}", file );
                proceed = true;
                break;
            }
            else
            {
                logger.debug( "Lock still held by: {}", ref );
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
                Thread.currentThread().interrupt();
                proceed = false;
            }
        }

        logger.trace( "<<<WAIT (any file activity): {}", proceed );
        return proceed;
    }

    private Object stackTrace()
    {
        if ( !logger.isTraceEnabled() )
        {
            return null;
        }

        final StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();

        int idx = 0;
        for ( final StackTraceElement elt : stackTrace )
        {
            idx++;
            if ( elt.getMethodName().equals( "stackTrace" ) )
            {
                break;
            }
        }

        final StackTraceElement[] trace = new StackTraceElement[stackTrace.length - idx];
        System.arraycopy( stackTrace, idx, trace, 0, trace.length );

        return new Object()
        {
            @Override
            public String toString()
            {
                return StringUtils.join( trace, "\n  " );
            }
        };
    }

    private final class StreamCallback
            extends AbstractStreamCallbacks
    {
        private final Logger logger = LoggerFactory.getLogger( getClass() );

        private final File file;

        private boolean joinable;

        StreamCallback( final File file, final boolean joinable )
        {
            this.file = file;
            this.joinable = joinable;
        }

        StreamCallback( final File file )
        {
            this.file = file;
        }

        @Override
        public void closed()
        {
            logger.trace( ">>>CLOSE/UNLOCK: {} at:\n\n  {}", file, stackTrace() );
            synchronized ( activeFiles )
            {
                final LockOwner ref = activeFiles.get( file );
                if ( ref != null )
                {
                    logger.trace( "Unlocking. Previously locked by: {}", ref.getThreadName() );

                    activeFiles.remove( file );
                    activeFiles.notifyAll();
                }
            }
            logger.trace( "<<<CLOSE/UNLOCK" );
        }

        @Override
        public void beforeClose()
        {
            if ( joinable )
            {
                synchronized ( joinableStreams )
                {
                    joinableStreams.remove( file );
                }
            }
        }
    }

    private final class ReportingTask
            extends TimerTask
    {
        @Override
        public void run()
        {
            final Map<File, CharSequence> activeLocks = getActiveLocks();
            if ( activeLocks.isEmpty() )
            {
                logger.trace( "No file locks to report." );
                return;
            }

            final StringBuilder sb = new StringBuilder();
            sb.append( "\n\nThe following file locks are still active:" );
            for ( final File file : activeLocks.keySet() )
            {
                sb.append( "\n" ).append( file ).append( " is owned by " ).append( activeLocks.get( file ) );
            }

            sb.append( "\n\n" );

            logger.info( sb.toString() );
        }
    }

}
