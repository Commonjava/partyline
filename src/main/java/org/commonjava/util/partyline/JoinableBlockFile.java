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

import org.commonjava.util.partyline.callback.StreamCallbacks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.UUID;


/**
 * Manages concurrent read/write access to a file, via {@link RandomAccessFile}, {@link FileChannel}, and careful
 * management of the read and write locations. Writes go to an in-memory buffer (8kb as of this writing, see CHUNK_SIZE)
 * then get flushed to the channel. Reads will read from the channel until they get to the last flushed point of the
 * writer, then they read from the in-memory buffer. Finally, when readers have read all the way through the in-memory
 * buffer and catch up with the writer, they wait for new input to be added to the buffer.
 * <br/>
 * {@link JoinableFile} instances keep a reference count of readers + writer (if there is one), and will not completely
 * close until all associated reader/writer streams close. When it does close, it uses {@link FileTree}'s internal
 * FileTreeCallbacks instance (passed into the constructor) to close any remaining wayward locks and cleanup the associated
 * state.
 * <br/>
 * <b>NOTE:</b> If the first access initializing a {@link JoinableFile} is a read operation, the flushed byte count is
 * set to the length of the file, and the in-memory buffer isn't used.
 * <br/>
 * <b>NOTE 2:</b> If the file is a directory, this {@link JoinableFile} is instantiated as a dummy that doesn't allow
 * anything to read / write.
 * <br/>
 * <b>NOTE 3:</b> This implementation uses NIO {@link FileLock} to try to lock the underlying filesystem.
 *
 * @author jdcasey
 */
public final class JoinableBlockFile
        implements AutoCloseable, Closeable, JoinableFileInterface
{
    private static final int CHUNK_SIZE = 1024 * 1024; // 1mb

//    private final FileLock fileLock;

    private final JoinableOutputStream output;

    private final Map<Integer, JoinInputStream> inputs = new ConcurrentHashMap<>();

    private AtomicLong flushed = new AtomicLong( 0 );

    private final String path;

    private final FileMeta blockFile;

    private FileBlock prevBlock;

    private FileBlock currBlock;

    private final DefaultCacheManager fileCache;

    private final DefaultCacheManager blockCache;

    private final StreamCallbacks callbacks;

    private volatile boolean closed = false;

    private volatile boolean joinable = true;

    private final LockOwner owner;

    private final FileOperationLock opLock;

    /**
     * Create any parent directories if necessary, then open the {@link RandomAccessFile} that will receive content on this stream. From that, init
     * the {@link FileChannel} that will be used to write content and map sections of the written file for reading in associated {@link JoinInputStream}
     * instances.
     * <br/>
     *
     * Initialize the {@link JoinableOutputStream} and {@link ByteBuffer} that will buffer content before sending it on
     * to the channel (in the {@link JoinableOutputStream#flush()} method).
     */
    JoinableBlockFile( final File target, final LockOwner owner, boolean doOutput )
            throws IOException
    {
        this( target, owner, null, doOutput, new FileOperationLock() );
    }

    /**
     * Create any parent directories if necessary, then open the {@link RandomAccessFile} that will receive content on this stream. From that, init
     * the {@link FileChannel} that will be used to write content and map sections of the written file for reading in associated {@link JoinInputStream}
     * instances.
     * <br/>
     * If writable, initialize the {@link JoinableOutputStream} and {@link ByteBuffer} that will buffer content before sending
     * it on to the channel (in the {@link JoinableOutputStream#flush()} method).
     * <br/>
     * If callbacks are available, use these to signal to a manager instance when the stream is flushed and when
     * the last joined input stream (or this stream, if there are none) closes.
     */
    JoinableBlockFile( final File target, final LockOwner owner, final StreamCallbacks callbacks, boolean doOutput, FileOperationLock opLock )
            throws IOException
    {
        this.owner = owner;
        this.path = target.getPath();
        this.callbacks = callbacks;
        this.opLock = opLock;

        target.getParentFile().mkdirs();

        Logger logger = LoggerFactory.getLogger( getClass() );
        logger.trace( "Trying to initialize JoinableFile to: {} using operation lock:\n\n{}", target, opLock );
        try
        {
            if ( target.isDirectory() )
            {
                logger.trace( "INIT: locking directory WITHOUT lock in underlying filesystem!" );
                output = null;
                blockFile = null;
                channel = null;
//                fileLock = null;
                joinable = false;
            }
            else if ( doOutput )
            {
                logger.trace( "INIT: read-write JoinableFile: {}", target );
                output = new JoinableOutputStream();
                blockFile = new FileMeta( this.path, UUID.randomUUID() );
            }
            else
            {
                logger.trace( "INIT: read-only JoinableFile: {}", target );
                output = null;
                logger.trace( "INIT: set flushed length to: {}", target.length() );
                flushed.set( target.length() );
                blockFile = new FileMeta( this.path, UUID.randomUUID() );
            }
        }
        catch ( OverlappingFileLockException e )
        {
            throw new IOException( "Cannot lock file: " + target + ". Reason: " + e.getMessage() + "\nLocked by: " + owner.getLockInfo(), e );
        }
    }

    LockOwner getLockOwner()
    {
        return owner;
    }

    // only public for testing purposes...
    public OutputStream getOutputStream()
    {
        return output;
    }

    boolean isJoinable()
    {
        return joinable;
    }

    /**
     * Return an {@link InputStream} instance that reads from the same {@link RandomAccessFile} that backs this output stream, and is tuned to listen
     * for notification that this stream is closed before signaling that it is out of content. The returned stream is of type {@link JoinInputStream}.
     */
    InputStream joinStream()
            throws IOException, InterruptedException
    {
        return lockAnd( (lock)->{
            if ( !joinable )
            {
                // if the channel is null, this is a directory lock.
                throw new IOException( "JoinableFile is not accepting join() operations" );
            }

            JoinInputStream result = new JoinInputStream( inputs.size() );
            inputs.put( result.hashCode(), result );

            Logger logger = LoggerFactory.getLogger( getClass() );
            logger.debug( "JOIN: {} (new joint count: {})", Thread.currentThread().getName(), inputs.size() );

            return result;
        });
    }

    /**
     * Lock the {@link java.util.concurrent.locks.ReentrantLock} instance embedded in this {@link JoinableFile}, then
     * execute the given operation. This prevents more than one thread from executing operations against state associated
     * with the file this {@link JoinableFile} manages.
     *
     * @param op The operation to execute, once the operation lock is acquired
     * @param <T> The result type of the given operation
     * @return The result of operation execution
     * @throws IOException
     * @throws InterruptedException
     *
     * @see FileTree#withOpLock(File, LockedFileOperation) for associated logic
     */
    private <T> T lockAnd(LockedFileOperation<T> op)
            throws IOException, InterruptedException
    {
        boolean locked = false;
        try
        {
            locked = opLock.lock();
            return op.execute( opLock );
        }
        finally
        {
            if ( locked )
            {
                opLock.unlock();
            }
        }
    }

    /**
     * Mark this stream as closed. Don't close the underlying channel if
     * there are still open input streams...allow their close methods to trigger that if the ref count drops 
     * to 0.
     */
    @Override
    public void close()
            throws IOException
    {
        try
        {
            lockAnd( (lock)->{
                Logger logger = LoggerFactory.getLogger( getClass() );
                if ( closed )
                {
                    logger.trace( "close() called, but is already closed." );
                    return null;
                }
                logger.trace( "close() called, marking as closed." );

                closed = true;

                if ( output != null && !output.isClosed() )
                {
                    logger.trace( "Closing output" );
                    output.close();
                }

                logger.trace( "joint count is: {}.", inputs.size() );
                if ( inputs.isEmpty() )
                {
                    logger.trace( "Joints closed, and output is closed...really closing." );
                    reallyClose();
                    owner.clearLocks();
                }

                return null;
            } );
        }
        catch ( InterruptedException e )
        {
            Logger logger = LoggerFactory.getLogger( getClass() );
            logger.warn( "Interrupted while closing: {}", getPath() );
        }
    }

    /**
     * After all associated {@link JoinInputStream}s are done, close down this stream's backing storage.
     */
    private void reallyClose()
            throws IOException
    {
        Logger logger = LoggerFactory.getLogger( getClass() );
        logger.trace( "Really closing JoinableFile: {}", path );

        try
        {
            lockAnd((lock)->{

                if ( callbacks != null )
                {
                    logger.trace( "calling beforeClose() on callbacks: {}", callbacks );
                    callbacks.beforeClose();
                }

                joinable = false;

                if ( output != null )
                {
                    logger.trace( "Setting length of: {} to written length: {}", path, flushed );
                    // TODO: Work out what to do with blocks here
                    /*randomAccessFile.setLength( flushed.get() );
                    randomAccessFile.getFD().sync(); */
                }
                logger.trace( "JoinableFile for: {} is really closed (by thread: {}).", path,
                              Thread.currentThread().getName() );

                if ( callbacks != null )
                {
                    logger.trace( "calling closed() on callbacks: {}", callbacks );
                    callbacks.closed();
                }

                return null;
            });
        }
        catch ( InterruptedException e )
        {
            logger.error( "Interrupted while closing: " + path, e );
        }
    }

    /**
     * Callback for use in {@link JoinInputStream} to notify this stream to decrement its count of associated input streams.
     * @throws IOException
     */
    private void jointClosed( JoinInputStream input, String originalThreadName )
            throws IOException
    {
        try
        {
            lockAnd( (lock)->{
                inputs.remove( input.hashCode() );

                Logger logger = LoggerFactory.getLogger( getClass() );
                logger.trace( "jointClosed() called in: {}, current joint count: {}", this, inputs.size() );
                if ( inputs.isEmpty() )
                {
                    if ( output == null || output.isClosed() )
                    {
                        logger.trace( "All input joint closed, and output is missing or closed. Really closing." );
                        closed = true;
                        reallyClose();
                    }
                }
                else
                {
                    owner.unlock( labelFor( false, originalThreadName ) );
//                    owner.unlock( originalThreadName );
                }

                return null;
            } );
        }
        catch ( InterruptedException e )
        {
            Logger logger = LoggerFactory.getLogger( getClass() );
            logger.warn( "Interrupted while closing reader joint of: {}", getPath() );
        }
    }

    /**
     * Retrieve the path that is managed in this instance.
     */
    String getPath()
    {
        return path;
    }

    boolean isOpen()
    {
        return !closed || !inputs.isEmpty();
    }

    public String reportOwnership()
    {
        StringBuilder sb = new StringBuilder();
        sb.append( "Path: " ).append( path );

        // TODO: implement isWriteLocked()
        sb.append( "\n" )
          .append( owner.isLocked() ? "LOCKED (" : "UNLOCKED (" )
          .append( isWriteLocked() ? "write)" : "read)" );

        sb.append( "\nStreams:" );
        if ( output != null )
        {
            sb.append( "\n\t- " ).append( output.reportWithOwner() );
        }

        /*
         * [JAVADOC] Iterators for ConcurrentHashMap return elements reflecting the state of the hash table at some point
         * or since the creation of the iterator/enumeration. They do not throw ConcurrentModificationException.
         * However, iterators are designed to be used by only one thread at a time.
         */
        inputs.forEach( (hashCode, instance)-> sb.append( "\n\t- " ).append( instance.reportWithOwner() ) );

        return sb.toString();
    }

    public static String labelFor( final boolean doOutput, String threadName )
    {
        return (doOutput ? "WRITE via " : "READ via ") + threadName;
    }

    private final class JoinableOutputStream
            extends OutputStream
    {
        private boolean closed;

        private ByteBuffer buf = ByteBuffer.allocateDirect( CHUNK_SIZE );

        private String originalThreadName = Thread.currentThread().getName();

        public String reportWithOwner()
        {
            return String.format( "output (%s)", originalThreadName );
        }

        /**
         * If the stream is marked as closed, throw {@link IOException}. If the INTERNAL buffer is full, call {@link #flush()}. Then, write the byte to
         * the buffer and increment the written-byte count.
         */

        @Override
        public void write ( final int b )
        {
            if ( (int) b == -1 )
                {
                    currBlock.setEOF();
                    doFlush( true );
                }
                if ( !currBlock.full() )
                {
                    currBlock.writeToBuffer( (byte) b );
                }
                else
                {
                    UUID newBlockID = UUID.randomUUID();
                    FileBlock block = new FileBlock( blockFile.getFileID(), newBlockID );
                    currBlock.setNextBlockID( newBlockID );
                    prevBlock = currBlock;
                    currBlock = block;

                    flush();

                    currBlock.writeToBuffer ( (byte) b );
                }
        }

        public void write( ByteBuffer buf )
                throws IOException
        {
            while ( buf.hasRemaining() )
            {
                byte b = buf.get();
                write( (int) b);
            }
        }

        /**
         * Empty the current buffer into the {@link FileChannel} and reinitialize it for filling. Increment the flushed-byte count, which is used as the
         * read limit for associated {@link JoinInputStream}s. Notify anyone listening that there is new content via {@link JoinableFile#notifyAll()}.
         */
        @Override
        public void flush()
                throws IOException
        {
            synchronized ( JoinableBlockFile.this )
            {
                if ( closed )
                {
                    throw new IOException( "Cannot write to closed stream!" );
                }
            }

            doFlush( false );
        }

        @Override
        public void close()
            throws IOException
        {
            Logger logger = LoggerFactory.getLogger( getClass() );
            logger.trace( "OUT ({}):: close() called", originalThreadName );

            if ( closed )
            {
                logger.trace( "OUT ({}):: already closed", originalThreadName );
                return;
            }
            doFlush( true );
            super.close();

            JoinableBlockFile.this.close();
        }

        public void doFlush( boolean eof )
            throws IOException
        {
            if ( eof )
            {
                blockCache.put( prevBlock.getBlockID(), prevBlock );
                blockCache.put ( currblock.getBlockID, currBlock );
                fileCache.put( fileMeta.getFileID(), fileMeta );
            }
            else
            {
                blockCache.put( prevBlock.getBlockID(), prevBlock );
            }
            synchronized ( JoinableBlockFile.this )
            {
                JoinableBlockFile.this.notifyAll();
            }
            if ( callbacks != null )
            {
                callbacks.flushed();
            }
        }

        boolean isClosed() {
            return closed;
        }
    }

    /**
     * {@link InputStream} associated with a particular {@link JoinableFile} instance. This stream reads content that the output stream has
     * already flushed to disk, and waits for new content to become available (or for the output stream to close). This allows multiple readers
     * when content is still being written to disk.
     */
    private final class JoinInputStream
            extends InputStream
    {
        private static final long MAX_BUFFER_SIZE = 5 * 1024 * 1024; // 5Mb.

        private long read = 0;

        private FileBlock block;

        private boolean closed = false;

        private final int jointIdx;

        private final String originalThreadName;

        private final long ctorTime;

        /**
         * Map the content already written to disk for reading. If the flushed count exceeds MAX_BUFFER_SIZE, use the max instead.
         */
        JoinInputStream( int jointIdx )
                throws IOException
        {
            this.jointIdx = jointIdx;
            block = blockFile.getFirstBlock();
            this.originalThreadName = Thread.currentThread().getName();
            this.ctorTime = System.nanoTime();
        }

        @Override
        public boolean equals( final Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( !( o instanceof JoinInputStream ) )
            {
                return false;
            }

            final JoinInputStream that = (JoinInputStream) o;

            if ( jointIdx != that.jointIdx )
            {
                return false;
            }
            if ( ctorTime != that.ctorTime )
            {
                return false;
            }
            return originalThreadName.equals( that.originalThreadName );
        }

        @Override
        public int hashCode()
        {
            int result = jointIdx;
            result = 31 * result + originalThreadName.hashCode();
            result = 31 * result + (int) ( ctorTime ^ ( ctorTime >>> 32 ) );
            return result;
        }

        /**
         * If this stream is in the process of closing, throw {@link IOException}. While the read-bytes count in this
         * stream equals the flushed-bytes count in the associated output stream, wait for new content. If the output stream closes while we're 
         * waiting, return -1. If the thread is interrupted while we're waiting, return -1.
         *
         * If the current mapped buffer has been completely read, map the next section of content from the file. Then read the next byte from the 
         * buffer, increment the read-bytes count, and return it.
         */
        @Override
        public int read()
                throws IOException
        {
            synchronized ( JoinableBlockFile.this )
            {
                if ( closed )
                {
                    throw new IOException( "Joint: " + jointIdx + "(" + originalThreadName + "): Cannot read from closed stream!" );
                }

                //                Logger logger = LoggerFactory.getLogger( getClass() );
                //                logger.trace( "Joint: {} READ: read-bytes count: {}, flushed-bytes count: {}", jointIdx, read, flushed );
                while ( read == flushed.get() )
                {
                    if ( output == null || JoinableBlockFile.this.closed )
                    {
                        // if the parent stream is closed, return EOF
                        return -1;
                    }

                    try
                    {
                        JoinableBlockFile.this.wait( 100 );
                    }
                    catch ( final InterruptedException e )
                    {
                        // if we're interrupted, return EOF
                        return -1;
                    }

                    //                    logger.trace( "Joint: {} READ2: read-bytes count: {}, flushed-bytes count: {}", jointIdx, read, flushed );
                }
            }

            if ( buf.position() == buf.limit() )
            {
                //                logger.trace( "Joint: {} READ: filling buffer from {} to {} bytes", jointIdx, read, (flushed-read) );
                // map more content from the file, reading past our read-bytes count up to the number of flushed bytes from the parent stream
                long end = flushed.get() - read > MAX_BUFFER_SIZE ? MAX_BUFFER_SIZE : flushed.get() - read;

                Logger logger = LoggerFactory.getLogger( getClass() );
                logger.trace( "Buffering {} - {} (size is: {})\n", read, read+end, flushed );

                buf = channel.map( MapMode.READ_ONLY, read,
                                   end );
            }

            // be extra careful...if the new buffer is empty, return EOF.
            if ( buf.position() == buf.limit() )
            {
                //                logger.trace( "Joint: {} READ: New buffer is empty! Return -1", jointIdx );
                return -1;
            }

            final int result = buf.get();
            read++;

            //            logger.trace( "Joint: {} Read count: {}, returning: {}", jointIdx, read, Integer.toHexString( result ) );
            // byte is signed in java. Converting to unsigned:
            return result & 0xff;
        }

        @Override
        public int read()
                throws IOException
        {
            synchronized ( JoinableBlockFile.this )
            {
                if ( closed )
                {
                    throw new IOException( "Joint: " + jointIdx + "(" + originalThreadName + "): Cannot read from closed stream!" );
                }
            }

            while ( read == )
        }

        /**
         * Mark this stream as closed to no further reads can proceed. Then, call {@link JoinableFile#jointClosed(JoinInputStream, String)} to notify the parent
         * output stream to decrement its open-reader count and notify anyone waiting in case a close is in progress.
         */
        @Override
        public void close()
                throws IOException
        {
            Logger logger = LoggerFactory.getLogger( getClass() );
            logger.trace( "Joint: {} ({}) close() called.", jointIdx, originalThreadName );
            if ( closed )
            {
                logger.trace( "Joint: {} ({}) already closed.", jointIdx, originalThreadName );
                return;
            }

            closed = true;
            super.close();

            jointClosed( this, originalThreadName );
        }

        int getJointIndex()
        {
            return jointIdx;
        }

        public String reportWithOwner()
        {
            return String.format( "input-%s (%s)", jointIdx, originalThreadName );
        }
    }

    @Override
    public String toString()
    {
        return "JoinableFile{" +
                "path='" + path + '\'' +
                '}';
    }
}
