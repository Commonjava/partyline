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
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.channels.FileLock;

/**
 * {@link OutputStream} implementation backed by a {@link RandomAccessFile} / {@link FileChannel} combination, and allowing multiple callers to "join"
 * and retrieve {@link InputStream} instances that can read the content as it becomes available. The {@link InputStream}s that are generated are tuned
 * to wait for content until the associated {@link JoinableFile} notifies them that it is closed.
 *
 * @author jdcasey
 */
public class JoinableFile
        implements AutoCloseable, Closeable
{
    private static final int CHUNK_SIZE = 1024 * 1024; // 1 mb

    private final FileChannel channel;

    private final FileLock fileLock;

    private final JoinableOutputStream output;

    private long flushed = 0;

    private int jointCount = 0;

    private final String path;

    private final RandomAccessFile randomAccessFile;

    private final StreamCallbacks callbacks;

    private boolean closed = false;

    private volatile boolean joinable = true;

    private final LockOwner owner;

    /**
     * Create any parent directories if necessary, then open the {@link RandomAccessFile} that will receive content on this stream. From that, init
     * the {@link FileChannel} that will be used to write content and map sections of the written file for reading in associated {@link JoinInputStream}
     * instances.
     * <br/>
     *
     * Initialize the {@link JoinableOutputStream} and {@link ByteBuffer} that will buffer content before sending it on
     * to the channel (in the {@link JoinableOutputStream#flush()} method).
     */
    public JoinableFile( final File target, final LockOwner owner, boolean doOutput )
            throws IOException
    {
        this( target, owner, null, doOutput );
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
    public JoinableFile( final File target, final LockOwner owner, final StreamCallbacks callbacks, boolean doOutput )
            throws IOException
    {
        this.owner = owner;
        this.path = target.getPath();
        this.callbacks = callbacks;

        target.getParentFile().mkdirs();

        Logger logger = LoggerFactory.getLogger( getClass() );
        if ( target.isDirectory() )
        {
            logger.trace( "INIT: locking directory WITHOUT lock in underlying filesystem!" );
            output = null;
            randomAccessFile = null;
            channel = null;
            fileLock = null;
            joinable = false;
        }
        else if ( doOutput )
        {
            logger.trace( "INIT: read-write JoinableFile: {}", target );
            output = new JoinableOutputStream();
            randomAccessFile = new RandomAccessFile( target, "rw" );
            channel = randomAccessFile.getChannel();
            fileLock = channel.lock( 0L, Long.MAX_VALUE, false );
        }
        else
        {
            logger.trace( "INIT: read-only JoinableFile: {}", target );
            output = null;
            logger.trace( "INIT: set flushed length to: {}", target.length() );
            flushed = target.length();
            randomAccessFile = new RandomAccessFile( target, "r" );
            channel = randomAccessFile.getChannel();
            fileLock = channel.lock( 0L, Long.MAX_VALUE, true );
        }
    }

    public LockOwner getLockOwner()
    {
        return owner;
    }

    public OutputStream getOutputStream()
    {
        return output;
    }

    public boolean isJoinable()
    {
        return joinable;
    }

    public boolean isDirectory()
    {
        return channel == null;
    }

    /**
     * Return an {@link InputStream} instance that reads from the same {@link RandomAccessFile} that backs this output stream, and is tuned to listen
     * for notification that this stream is closed before signaling that it is out of content. The returned stream is of type {@link JoinInputStream}.
     */
    public synchronized InputStream joinStream()
            throws IOException
    {
        if ( !joinable )
        {
            // if the channel is null, this is a directory lock.
            throw new IOException( "JoinableFile is not accepting join() operations. (" +
                                           ( channel == null ?
                                                   "It's a locked directory" :
                                                   "It's in the process of closing." ) + ")" );
        }

        Logger logger = LoggerFactory.getLogger( getClass() );
        logger.debug( "JOIN: {}", Thread.currentThread().getName() );
        jointCount++;
        return new JoinInputStream( jointCount );
    }

    private void checkWritable()
            throws IOException
    {
        if ( output == null )
        {
            throw new IOException( "JoinableFile is not writable!" );
        }
    }

    /**
     * Write locks happen when either a directory is locked, or the file was locked with doOutput == true.
     */
    public boolean isWriteLocked()
    {
        // if the channel is null, this is a directory lock.
        return channel == null || output != null;
    }

    /**
     * Mark this stream as closed. Don't close the underlying channel if
     * there are still open input streams...allow their close methods to trigger that if the ref count drops 
     * to 0.
     */
    @Override
    public synchronized void close()
            throws IOException
    {
        closed = true;

        if ( output != null && !output.closed )
        {
            output.close();
        }

        if ( channel == null || jointCount <= 0 )
        {
            reallyClose();
        }
    }

    /**
     * After all associated {@link JoinInputStream}s are done, close down this stream's backing storage.
     */
    private synchronized void reallyClose()
            throws IOException
    {
        Logger logger = LoggerFactory.getLogger( getClass() );
        logger.trace( "Really closing JoinableFile: {}", path );
        if ( callbacks != null )
        {
            logger.trace( "calling beforeClose() on callbacks: {}", callbacks );
            callbacks.beforeClose();
        }

        joinable = false;

        if ( output != null )
        {
            logger.trace( "Setting length of: {} to written length: {}", path, flushed );
            randomAccessFile.setLength( flushed );
        }

        // if the channel is null, this is a directory lock.
        if ( channel != null )
        {
            logger.trace( "Closing underlying channel / random-access file..." );
            try
            {
                if ( channel.isOpen() )
                {
                    fileLock.release();
                    channel.close();
                }
                randomAccessFile.close();
            }
            catch ( ClosedChannelException e )
            {
                logger.debug( "Lock release failed on closed channel.", e );
            }
        }

        logger.trace( "JoinableFile for: {} is really closed (by thread: {}).", path,
                      Thread.currentThread().getName() );

        // FIXME: This should NEVER be unlocked!!
        //        if ( lock.isLocked() )
        //        {
        //        lock.unlock();
        //        }

        if ( callbacks != null )
        {
            logger.trace( "calling closed() on callbacks: {}", callbacks );
            callbacks.closed();
        }
    }

    /**
     * Callback for use in {@link JoinInputStream} to notify this stream to decrement its count of associated input streams.
     * @throws IOException
     */
    private synchronized void jointClosed()
            throws IOException
    {
        jointCount--;

        Logger logger = LoggerFactory.getLogger( getClass() );
        logger.trace( "jointClosed() called in: {}, current joint count: {}", this, jointCount );
        if ( jointCount <= 0 )
        {
            if ( output == null || output.closed )
            {
                closed = true;
                reallyClose();
            }
        }
    }

    /**
     * Retrieve the path that is managed in this instance.
     */
    public String getPath()
    {
        return path;
    }

    public boolean isOpen()
    {
        return !closed || jointCount > 0;
    }

    public boolean isOwnedByCurrentThread()
    {
        return Thread.currentThread().getId() == owner.getThreadId();
    }

    public boolean isOwnedBy( long ownerId )
    {
        return ownerId == owner.getThreadId();
    }

    private final class JoinableOutputStream
            extends OutputStream
    {
        private boolean closed;

        private ByteBuffer buf = ByteBuffer.allocateDirect( CHUNK_SIZE );

        /**
         * If the stream is marked as closed, throw {@link IOException}. If the INTERNAL buffer is full, call {@link #flush()}. Then, write the byte to
         * the buffer and increment the written-byte count.
         */
        @Override
        public void write( final int b )
                throws IOException
        {
            if ( closed )
            {
                throw new IOException( "Cannot write to closed stream!" );
            }

            if ( buf.position() == buf.capacity() )
            {
                flush();
            }

            buf.put( (byte) ( b & 0xff ) );
        }

        /**
         * Empty the current buffer into the {@link FileChannel} and reinitialize it for filling. Increment the flushed-byte count, which is used as the
         * read limit for associated {@link JoinInputStream}s. Notify anyone listening that there is new content via {@link JoinableFile#notifyAll()}.
         */
        @Override
        public void flush()
                throws IOException
        {
            buf.flip();
            int count = channel.write( buf );
            buf.clear();

            super.flush();

            synchronized ( JoinableFile.this )
            {
                flushed += count;
                JoinableFile.this.notifyAll();
            }

            if ( callbacks != null )
            {
                callbacks.flushed();
            }
        }

        /**
         * Flush anything in the current buffer. Mark this stream as closed. Don't close the underlying channel if
         * there are still open input streams...allow their close methods to trigger that if the ref count drops
         * to 0.
         */
        @Override
        public void close()
                throws IOException
        {
            closed = true;
            flush();
            super.close();
            JoinableFile.this.close();
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

        private ByteBuffer buf;

        private boolean closed = false;

        private int jointIdx;

        /**
         * Map the content already written to disk for reading. If the flushed count exceeds MAX_BUFFER_SIZE, use the max instead.
         */
        JoinInputStream( int jointIdx )
                throws IOException
        {
            this.jointIdx = jointIdx;
            buf = channel.map( MapMode.READ_ONLY, 0, flushed > MAX_BUFFER_SIZE ? MAX_BUFFER_SIZE : flushed );
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
            if ( closed )
            {
                throw new IOException( "Joint: " + jointIdx + ": Cannot read from closed stream!" );
            }

            synchronized ( JoinableFile.this )
            {
                //                Logger logger = LoggerFactory.getLogger( getClass() );
                //                logger.trace( "Joint: {} READ: read-bytes count: {}, flushed-bytes count: {}", jointIdx, read, flushed );
                while ( read == flushed )
                {
                    if ( output == null || JoinableFile.this.closed )
                    {
                        // if the parent stream is closed, return EOF
                        return -1;
                    }

                    try
                    {
                        JoinableFile.this.wait( 100 );
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
                long end = flushed > MAX_BUFFER_SIZE ? MAX_BUFFER_SIZE : flushed - read;
                if ( (read + end ) > channel.size() )
                {
                    end = channel.size() - read;
                }

                Logger logger = LoggerFactory.getLogger( getClass() );
                logger.trace( "Buffering {} - {} (size is: {})\n", read, read+end, channel.size() );

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

        /**
         * Mark this stream as closed to no further reads can proceed. Then, call {@link JoinableFile#jointClosed()} to notify the parent
         * output stream to decrement its open-reader count and notify anyone waiting in case a close is in progress.
         */
        @Override
        public void close()
                throws IOException
        {
            Logger logger = LoggerFactory.getLogger( getClass() );
            logger.trace( "Joint: {} close() called.", jointIdx );
            closed = true;
            super.close();
            jointClosed();
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
