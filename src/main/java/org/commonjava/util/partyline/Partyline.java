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
package org.commonjava.util.partyline;

import org.commonjava.cdi.util.weft.ThreadContext;
import org.commonjava.util.partyline.lock.LockLevel;
import org.commonjava.util.partyline.impl.local.RandomAccessJF;
import org.commonjava.util.partyline.impl.local.RandomAccessJFS;
import org.commonjava.util.partyline.spi.JoinableFilesystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.apache.commons.lang.StringUtils.join;

import static org.commonjava.util.partyline.lock.LockLevel.read;
import static org.commonjava.util.partyline.lock.local.LocalLockOwner.getLockReservationName;

/**
 * File manager that attempts to manage read/write locks in the presence of output streams that will allow simultaneous access to read the content
 * they are writing. Also allows the user to lock/unlock files manually in case they need to be used outside the normal streaming use cases.
 *
 * @author jdcasey
 */
public class Partyline
{

    public static final String PARTYLINE_OPEN_FILES = "partyline-open-files";

    public static final long DEFAULT_TIMEOUT = 1000;

    private static final int MAX_CLEANUP_ITERATIONS = 3;
    private static final long CLEANUP_ITERATION_WAIT = 1000;

    /**
     * Consumer of {@link ThreadContext} which clears the {@link #PARTYLINE_OPEN_FILES} map by closing all open files
     * in that map, trying each one up to {@link #MAX_CLEANUP_ITERATIONS} times before giving up. Between cleanup
     * iterations, it will pause {@link #CLEANUP_ITERATION_WAIT} milliseconds.
     *
     * @since 1.9.5
     */
    private static final Consumer<ThreadContext> FILE_CLEANUP = (tc)->{
        Logger logger = LoggerFactory.getLogger( Partyline.class );

        Map<String, WeakReference<Closeable>> open =
                (Map<String, WeakReference<Closeable>>) tc.remove( PARTYLINE_OPEN_FILES );

        if ( open != null )
        {
            int iterations = 0;
            while( open.size() > 0 && iterations < MAX_CLEANUP_ITERATIONS )
            {
                for(String key: new HashSet<>( open.keySet()))
                {
                    WeakReference<Closeable> ref = open.remove( key );
                    Closeable c = ref == null ? null : ref.get();

                    if ( c != null )
                    {
                        try
                        {
                            c.close();
                        }
                        catch ( IOException ex )
                        {
                            logger.error( "Failed to close: " + key + ". Will retry.", ex );
                            open.put( key, ref );
                        }
                    }
                }

                if ( !open.isEmpty() )
                {
                    try
                    {
                        Thread.sleep( CLEANUP_ITERATION_WAIT );
                    }
                    catch ( InterruptedException e )
                    {
                        logger.warn("JoinableFileManager cleanup routine interrupted! Aborting.");
                        break;
                    }
                }

                iterations++;
            }

            if ( !open.isEmpty() )
            {
                logger.error( "JoinableFileManager cleanup routine giving up! {} files remain unclosed:\n\n  - {}\n\n",
                              open.size(), join( open.keySet(), "\n  - " ) );
            }
        }
    };

    private static final String MANUAL_LOCK_LABEL = "Manual lock";

    private final Logger logger = LoggerFactory.getLogger( getClass() );

    private final FileTree locks;

    private final Timer timer;

    private ReportingTask reporter;

    public Partyline()
    {
        this( new RandomAccessJFS() );
    }

    public Partyline(JoinableFilesystem filesystem)
    {
        this.timer = new Timer( true );
        this.locks = new FileTree( filesystem );
    }

    FileTree getFileTree()
    {
        return locks;
    }

    /**
     * This method is used to cleanup after an operation, just to be double sure we don't complete a request and leave
     * file streams open. This file manager is designed to work in an environment where a stream may be handed off to
     * a separate thread for reading / writing, while the original thread does something else. However, when all threads
     * related to a particular call or user request are finished, we need to ensure that request gets cleaned up. This
     * method enables that.
     *
     * @see #FILE_CLEANUP
     */
    public void cleanupCurrentThread()
    {
        // NOP, now handled by ThreadContext finalizer.
    }

    /**
     * Begin periodic reporting (to log output) on active file locks in the system. This is intended to make it easier
     * to see when things are being left active even after the call that initiated them is complete.
     */
    public synchronized void startReporting()
    {
        startReporting( 0, 10000 );
    }

    /**
     * Begin periodic reporting (to log output) on active file locks in the system. This is intended to make it easier
     * to see when things are being left active even after the call that initiated them is complete.
     *
     * This variant gives the embedding system more control over how often the report happens.
     * @param delay in milliseconds, the initial delay before reporting
     * @param period in milliseconds, the periodic delay between reports
     */
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

    /**
     * Turn off periodic reporting of active file locks.
     *
     * @see #startReporting()
     */
    public synchronized void stopReporting()
    {
        if ( reporter != null )
        {
            logger.info( "Stopping file-lock statistics reporting." );
            reporter.cancel();
        }
    }

    /**
     * Retrieve information about the active file locks in the system.
     *
     * @return Map of File to information about the locks pertaining to that file.
     */
    public Map<File, CharSequence> getActiveLocks()
    {
        final Map<File, CharSequence> active = new HashMap<>();

        locks.forAll( ( jf ) -> active.put( new File( jf.getPath() ), jf.reportOwnership() ));

        return active;
    }

    /**
     * If the file isn't marked as active, create a new {@link RandomAccessJF} to the specified file and pass it back to
     * the user.
     */
    public OutputStream openOutputStream( final File file )
            throws IOException, InterruptedException
    {
        return openOutputStream( file, -1 );
    }

    /**
     * If the file isn't marked as active, create a new {@link RandomAccessJF} to the specified file and pass it back to
     * the user. If the file is locked, wait for the specified milliseconds before giving up.
     */
    public OutputStream openOutputStream( final File file, final long timeout )
            throws IOException, InterruptedException
    {
        logger.trace( ">>>OPEN OUTPUT: {} with timeout: {}", file, timeout );

        OutputStream stream = locks.setOrJoinFile( file, true, timeout, TimeUnit.MILLISECONDS, ( result ) -> {
            if ( result == null )
            {
                throw new IOException( "Could not open output stream to: " + file + " in " + timeout + "ms." );
            }

            return result.getOutputStream();
        } );

        addToContext( "OUTPUT@" + System.nanoTime() + ": " + file, stream );

        return stream;
    }

    /**
     * Delete the given file, waiting until the file can be locked for deletion
     */
    public boolean tryDelete( File file )
            throws IOException, InterruptedException
    {
        return tryDelete( file, -1 );
    }

    /**
     * Delete the given file, waiting until the file can be locked for deletion
     *
     * @param file The file to delete
     * @param timeout Timeout (milliseconds) for attempt to lock file for deletion
     */
    public boolean tryDelete( File file, long timeout )
            throws IOException, InterruptedException
    {
        logger.trace( ">>>DELETE: {}", file, timeout );
        boolean result = locks.delete( file, timeout, TimeUnit.MILLISECONDS );
        logger.trace( "<<<DELETE (Result: {}, file exists? {})", result, file.exists() );
        return result;
    }

    /**
     * If there is an active {@link RandomAccessJF}, call {@link RandomAccessJF#joinStream()} and return it to the user.
     * Otherwise, open a new {@link FileInputStream} to the specified file and pass the result back to the user.
     */
    public InputStream openInputStream( final File file )
            throws IOException, InterruptedException
    {
        return openInputStream( file, 0 );
    }

    /**
     * If there is an active {@link RandomAccessJF}, call {@link RandomAccessJF#joinStream()} and return it to the user.
     * Otherwise, open a new {@link FileInputStream} to the specified file and pass the result back to the user. If the
     * file is locked for reads, wait for the specified milliseconds before giving up.
     */
    public InputStream openInputStream( final File file, final long timeout )
            throws IOException, InterruptedException
    {
        logger.trace( ">>>OPEN INPUT: {} with timeout: {}", file, timeout );
        AtomicReference<InterruptedException> interrupt = new AtomicReference<>();
        InputStream stream = locks.setOrJoinFile( file, false, timeout, TimeUnit.MILLISECONDS, ( result ) -> {
            if ( result == null )
            {
                throw new IOException( "Could not open input stream to: " + file + " in " + timeout + "ms." );
            }

            try
            {
                return result.joinStream();
            }
            catch ( InterruptedException e )
            {
                interrupt.set( e );
            }

            return null;
        } );

        InterruptedException ie = interrupt.get();
        if ( ie != null )
        {
            throw ie;
        }

        addToContext( "INPUT@" + System.nanoTime() + ": " + file, stream );

        return stream;
    }

    /**
     * Add the specified file path (and stream/closeable) to the map attached to the current {@link ThreadContext}
     * instance. This will enable {@link #cleanupCurrentThread()} later.
     *
     * @param name The file path / usage key to track
     * @param closeable The stream / other closeable to track for cleanup
     */
    private void addToContext( String name, Closeable closeable )
    {
        // [jdcasey]: Disabling this to avoid stream-closing problems associated with threaded Indy execution.
//        logger.debug( "Adding {} to closeable set in ThreadContext", name );
//
//        ThreadContext threadContext = ThreadContext.getContext( false );
//        if ( closeable != null && threadContext != null )
//        {
//            synchronized ( threadContext )
//            {
//                Map<String, WeakReference<Closeable>> open = (Map<String, WeakReference<Closeable>>) threadContext.get( PARTYLINE_OPEN_FILES );
//                if ( open == null )
//                {
//                    open = new HashMap<>();
//                    threadContext.put( PARTYLINE_OPEN_FILES, open ); // FILE_CLEANUP will remove it
//                }
//                open.put( name, new WeakReference<>( closeable ) );
//
//                threadContext.registerFinalizer( FILE_CLEANUP );
//            }
//        }
    }

    /**
     * Manually lock the specified file to prevent opening any streams via this manager (until manually unlocked).
     *
     * @param file The file to lock
     * @param timeout Timeout (milliseconds) to wait in acquiring the lock; fail if this expires without a lock
     * @param lockLevel The type of lock to acquire (read, write, delete)
     * @param operationName A label for the operation, to aid in debugging of stuck active locks.
     */
    @Deprecated
    public boolean lock( final File file, long timeout, LockLevel lockLevel, String operationName )
            throws InterruptedException
    {
        return lock( file, timeout, lockLevel );
    }

    public boolean lock( final File file, long timeout, LockLevel lockLevel )
            throws InterruptedException
    {
        logger.trace( ">>>MANUAL LOCK: {}", file );
        boolean result = locks.tryLock( file, MANUAL_LOCK_LABEL, lockLevel, timeout, TimeUnit.MILLISECONDS );
        logger.trace( "<<<MANUAL LOCK (result: {})", result );

        return result;
    }

    public boolean unlock( final File file )
            throws IOException
    {
        logger.trace( ">>>MANUAL UNLOCK: {} by: {}", file, getLockReservationName() );
        boolean result = locks.unlock( file, MANUAL_LOCK_LABEL );

        if ( result )
        {
            logger.trace( "<<<MANUAL UNLOCK (success)" );
        }
        else
        {
            logger.trace( "<<<MANUAL UNLOCK (failed)" );
        }

        return result;
    }

    public boolean isLockedByCurrentThread( File file )
    {
        return locks.isLockedByCurrentThread( file );
    }

    /**
     * Check if the specified file is locked against write operations. Files are write-locked if any other file access
     * is active.
     *
     * @see LockLevel
     */
    public boolean isWriteLocked( final File file )
    {
        LockLevel lockLevel = locks.getLockLevel( file );
        return lockLevel != null && lockLevel.ordinal() >= read.ordinal();
    }

    /**
     * The only time reads are not allowed is when the file is locked for deletion.
     *
     * @see LockLevel
     */
    public boolean isReadLocked( final File file )
    {
        return locks.getLockLevel( file ) == LockLevel.delete;
    }

    /**
     * Retrieve lock times for one single thread context of the specified file
     *
     * @return the lock times of file for single thread context.
     */
    public int getContextLockCount( final File file )
    {
        return locks.getContextLockCount( file );
    }

    /**
     * Wait the specified timeout milliseconds for write access on the specified file to become available. Return false
     * if the timeout elapses without the file becoming available for writes.
     *
     * @see #isWriteLocked(File)
     */
    public boolean waitForWriteUnlock( final File file )
            throws InterruptedException
    {
        return waitForWriteUnlock( file, -1 );
    }

    /**
     * Wait the specified timeout milliseconds for write access on the specified file to become available. Return false
     * if the timeout elapses without the file becoming available for writes.
     *
     * @see #isWriteLocked(File)
     */
    // FIXME: How do we prevent new incoming lock requests while we wait?
    public boolean waitForWriteUnlock( final File file, long timeout )
            throws InterruptedException
    {
        long to = timeout < 1 ? DEFAULT_TIMEOUT : timeout;

        logger.trace( ">>>WAIT (write unlock): {} with timeout: {}", file, to );

        long end = System.currentTimeMillis() + to;
        boolean result = false;
        while ( System.currentTimeMillis() < end )
        {
            LockLevel lockLevel = locks.getLockLevel( file );
            if ( lockLevel == null )
            {
                result = true;
                break;
            }

            synchronized ( locks )
            {
                locks.wait( 100 );
            }

            lockLevel = locks.getLockLevel( file );
            if ( lockLevel == null )
            {
                result = true;
                break;
            }
        }

        logger.trace( "<<<WAIT (write unlock) result: {}", result );
        return result;
    }

    /**
     * Wait the specified timeout milliseconds for read access on the specified file to become available. Return false
     * if the timeout elapses without the file becoming available for reads. If a {@link RandomAccessJF} is available for
     * the file, don't wait (immediately return true).
     *
     * @see #isReadLocked(File)
     */
    public boolean waitForReadUnlock( final File file, final long timeout )
            throws InterruptedException
    {
        long to = timeout < 1 ? DEFAULT_TIMEOUT : timeout;

        logger.trace( ">>>WAIT (read unlock): {} with timeout: {}", file, to );

        long end = System.currentTimeMillis() + to;
        boolean result = false;
        while ( System.currentTimeMillis() < end )
        {
            LockLevel lockLevel = locks.getLockLevel( file );
            if ( lockLevel != LockLevel.delete )
            {
                result = true;
                break;
            }

            synchronized ( locks )
            {
                locks.wait( 100 );
            }

            lockLevel = locks.getLockLevel( file );
            if ( lockLevel != LockLevel.delete )
            {
                result = true;
                break;
            }
        }

        logger.trace( "<<<WAIT (read unlock) result: {}", result );
        return result;
    }

    /**
     * Wait the specified timeout milliseconds for read access on the specified file to become available. Return false
     * if the timeout elapses without the file becoming available for reads. If a {@link RandomAccessJF} is available for
     * the file, don't wait (immediately return true).
     *
     * @see #isReadLocked(File)
     */
    public boolean waitForReadUnlock( final File file )
            throws InterruptedException
    {
        return waitForReadUnlock( file, -1 );
    }

    /**
     * {@link TimerTask} implementation that handles reporting active file locks to the logging output.
     */
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
                sb.append( "\n" )
                  .append( file )
                  .append( "\n-------------------------------\n" )
                  .append( activeLocks.get( file ) )
                  .append( "\n\n" );
            }

            logger.info( sb.toString() );
        }
    }

}
