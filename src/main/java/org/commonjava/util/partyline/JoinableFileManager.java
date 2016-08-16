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

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.commonjava.util.partyline.callback.AbstractStreamCallbacks;
import org.commonjava.util.partyline.callback.CallbackInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

/**
 * File manager that attempts to manage read/write locks in the presence of output streams that will allow simultaneous access to read the content
 * they are writing. Also allows the user to lock/unlock files manually in case they need to be used outside the normal streaming use cases.
 *
 * @author jdcasey
 */
public class JoinableFileManager
{

    private final Logger logger = LoggerFactory.getLogger( getClass() );

    private final Map<File, JoinableFile> streams = new ConcurrentHashMap<>();

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
        for ( final File f : streams.keySet() )
        {
            synchronized ( streams )
            {
                JoinableFile jf = streams.get( f );
                if ( jf != null && jf.isOwnedByCurrentThread() )
                {
                    streams.remove( f );
                    streams.notifyAll();

                    final StringBuilder sb = new StringBuilder();
                    LockOwner owner = jf.getLockOwner();
                    sb.append( "CLEARING ORPHANED LOCK:\nFile: " )
                      .append( f )
                      .append( "\nOwned by thread: " )
                      .append( owner.getThreadName() )
                      .append( " (ID: " )
                      .append( owner.getThreadId() )
                      .append( ")" )
                      .append( "\nLock type: " )
                      .append( jf.isWriteLocked() ? "WRITE" : "READ" )
                      .append( "\nLocked at:\n" );

                    for ( final StackTraceElement elt : owner.getLockOrigin() )
                    {
                        sb.append( "\n  " ).append( elt );
                    }

                    sb.append( "\n\n" );

                    logger.warn( sb.toString() );

                    IOUtils.closeQuietly( jf );
                }
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
        synchronized ( streams )
        {
            afs = new HashSet<>( streams.keySet() );
        }

        for ( final File f : afs )
        {
            final StringBuilder owner = new StringBuilder();

            JoinableFile jf = streams.get( f );
            final LockOwner ref = jf.getLockOwner();

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

            if ( jf.isWriteLocked() )
            {
                owner.append( " (WRITE)" );
            }
            else
            {
                owner.append( " (READ)" );
            }

            active.put( f, owner );
        }

        return active;
    }

    /**
     * If the file isn't marked as active, create a new {@link JoinableFile} to the specified file and pass it back to the user.
     */
    public OutputStream openOutputStream( final File file )
            throws IOException
    {
        return openOutputStream( file, -1 );
    }

    /**
     * If the file isn't marked as active, create a new {@link JoinableFile} to the specified file and pass it back to the user. If the file is locked, wait for the specified milliseconds before giving up.
     */
    public OutputStream openOutputStream( final File file, final long timeout )
            throws IOException
    {
        logger.trace( ">>>OPEN OUTPUT: {} with timeout: {}", file, timeout );
        synchronized ( streams )
        {
            final boolean proceed = timeout > 0 ? waitForFile( file, timeout ) : waitForFile( file );
            if ( !proceed )
            {
                logger.trace( "<<<OPEN OUTPUT (timeout)" );
                return null;
            }

            final JoinableFile jf = new JoinableFile( file, new StreamCallback( file, true ), true );
            streams.put( file, jf );

            logger.debug( "Locked by: {}", Thread.currentThread().getName() );
            OutputStream out = jf.getOutputStream();

            streams.notifyAll();
            logger.trace( "<<<OPEN OUTPUT" );

            return out;
        }
    }

    /**
     * If there is an active {@link JoinableFile}, call {@link JoinableFile#joinStream()} and return it to the user. Otherwise, open
     * a new {@link FileInputStream} to the specified file, wrap it in a {@link CallbackInputStream} to notify this manager when it closes, and pass
     * the result back to the user.
     */
    public InputStream openInputStream( final File file )
            throws FileNotFoundException, IOException
    {
        return openInputStream( file, -1 );
    }

    /**
     * If there is an active {@link JoinableFile}, call {@link JoinableFile#joinStream()} and return it to the user. Otherwise, open
     * a new {@link FileInputStream} to the specified file, wrap it in a {@link CallbackInputStream} to notify this manager when it closes, and pass
     * the result back to the user. If the file is locked for reads, wait for the specified milliseconds before giving up.
     */
    public InputStream openInputStream( final File file, final long timeout )
            throws FileNotFoundException, IOException
    {
        synchronized ( streams )
        {
            logger.trace( ">>>OPEN INPUT: {} with timeout: {}", file, timeout );

            JoinableFile joinable = streams.get( file );

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

                joinable = new JoinableFile( file, new StreamCallback( file, true ), false );
                streams.put( file, joinable );

                InputStream in = joinable.joinStream();
                streams.notifyAll();

                logger.trace( "<<<OPEN INPUT (raw), called from:\n  {}", stackTrace() );
                return in;
            }
        }
    }

    /**
     * Manually lock the specified file to prevent opening any streams via this manager (until manually unlocked).
     */
    public boolean lock( final File file, boolean writeLock )
            throws IOException
    {
        logger.trace( ">>>MANUAL LOCK: {} at:\n  {}", file, stackTrace() );
        synchronized ( streams )
        {
            JoinableFile jf = streams.get( file );
            if ( jf != null && jf.isOpen() )
            {
                logger.trace( "<<<MANUAL LOCK (failed)" );
                return false;
            }

            // TODO: I don't think this is necessary if we're only managing JoinableFile.
//            if ( jf != null )
//            {
//                // if we get here, the ref is non-null but dead. Clear it.
//                IOUtils.closeQuietly( jf );
//            }

            logger.debug( "Locked by: {}", Thread.currentThread().getName() );

            streams.put( file, new JoinableFile( file, new StreamCallback( file, true ), writeLock ) );
            streams.notifyAll();
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
        synchronized ( streams )
        {
            // TODO: atomic, no sync needed?
            JoinableFile jf = streams.get( file );
            if ( jf != null )
            {
                logger.warn( "Manual unlock called for: {}. This may not be safe!", file );
            }
            else
            {
                logger.trace( "<<<MANUAL UNLOCK (not locked)" );
                return true;
            }

            LockOwner ref = jf.getLockOwner();
            if ( !jf.isOwnedByCurrentThread() )
            {
                logger.warn( "Unlock attempt on file: {} by different thread!\n  locker: {}\n  unlocker: {})", file,
                             ref.getThreadName(), Thread.currentThread().getName() );

                logger.trace( "<<<MANUAL LOCK (allowed unlock attempt by different thread! locker: {}, unlocker: {})",
                              ref.getThreadName(), Thread.currentThread().getName() );
            }

            IOUtils.closeQuietly( jf );
            streams.notifyAll();
            logger.debug( "Unlocked by: {}. Previously locked by: {}", Thread.currentThread().getName(),
                          ref.getThreadName() );

        }

        logger.trace( "<<<MANUAL UNLOCK (success)" );
        return true;
    }

    /**
     * Check if the specified file is locked against write operations. Files are write-locked if any other file access is active.
     */
    public boolean isWriteLocked( final File file )
    {
        JoinableFile jf = streams.get( file );
        return jf != null;
    }

    /**
     * Never read-locked...we can always join the read (or write) operation in progress.
     */
    public boolean isReadLocked( final File file )
    {
        return false;
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
        synchronized ( streams )
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
        synchronized ( streams )
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
     * the file becoming available for reads. If a {@link JoinableFile} is available for the file, don't wait (immediately return true).
     *
     * @see #isReadLocked(File)
     */
    public boolean waitForReadUnlock( final File file, final long timeout )
    {
        return true;
    }

    /**
     * Wait the specified timeout milliseconds for read access on the specified file to become available. Return false if the timeout elapses without
     * the file becoming available for reads. If a {@link JoinableFile} is available for the file, don't wait (immediately return true).
     *
     * @see #isReadLocked(File)
     */
    public boolean waitForReadUnlock( final File file )
    {
        return true;
    }

    private boolean waitForFile( final File file )
    {

        return waitForFile( file, -1 );
    }

    private boolean waitForFile( final File file, final long timeout )
    {
        logger.trace( ">>>WAIT (any file activity): {} with timeout: {}", file, timeout );
        JoinableFile jf = streams.get( file );
        if ( jf == null )
        {
            logger.trace( "<<<WAIT (any file activity): Not locked" );
            return true;
        }

        String caller = logger.isDebugEnabled() ? truncatedStackTrace() : null;
        logger.trace( "wait called from:\n  {}", caller );

        boolean proceed = false;
        //        System.out.println( "Waiting (" + ( timeout < 0 ? "indeterminate time" : timeout + "ms" ) + ") for: " + file );

        final long ends = timeout < 0 ? -1 : System.currentTimeMillis() + timeout;
        while ( ends < 0 || System.currentTimeMillis() < ends )
        {
            jf = streams.get( file );
            if ( jf == null )
            {
                logger.debug( "Lock cleared for: {}\n\n(Lock was requested by:\n{})", file, caller );
                proceed = true;
                break;
            }
            else
            {
                logger.debug( "Lock still held by: {}\n\n(Lock was requested by:\n{})", jf.getLockOwner().getThreadName(), caller );
            }

            try
            {
                streams.wait( ( timeout > 0 && timeout < 1000 ) ? timeout : 1000 );
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

    private String truncatedStackTrace()
    {
        if ( logger.isDebugEnabled() )
        {
            List<StackTraceElement> elements = Arrays.asList( Thread.currentThread().getStackTrace() );
            if ( elements.size() > 8 )
            {
                elements = elements.subList( 2, 7 );
            }

            return StringUtils.join( elements, "\n  " );
        }
        else
        {
            return "stacktrace disabled";
        }
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

        StreamCallback( final File file, final boolean joinable )
        {
            this.file = file;
        }

        @Override
        public void closed()
        {
        }

        @Override
        public void beforeClose()
        {
            logger.trace( ">>>beforeClose() :: CLOSE/UNLOCK: {} at:\n\n  {}", file, stackTrace() );
            synchronized ( streams )
            {
                logger.trace( "Removing file from joinableStreams." );
                streams.remove( file );
            }
            logger.trace( "<<<beforeClose() :: CLOSE/UNLOCK" );
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
