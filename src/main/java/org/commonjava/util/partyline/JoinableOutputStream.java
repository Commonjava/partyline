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
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import org.commonjava.util.partyline.callback.StreamCallbacks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link OutputStream} implementation backed by a {@link RandomAccessFile} / {@link FileChannel} combination, and allowing multiple callers to "join"
 * and retrieve {@link InputStream} instances that can read the content as it becomes available. The {@link InputStream}s that are generated are tuned
 * to wait for content until the associated {@link JoinableOutputStream} notifies them that it is closed.
 * 
 * @author jdcasey
 */
public class JoinableOutputStream
    extends OutputStream
{
    private final Logger logger = LoggerFactory.getLogger( getClass() );

    private static final int CHUNK_SIZE = 1024 * 1024; // 1 mb

    private final FileChannel channel;

    private long written = 0;

    private long flushed = 0;

    private final ByteBuffer buf;

    private int jointCount = 0;

    private final File file;

    private final RandomAccessFile randomAccessFile;

    private final StreamCallbacks callbacks;

    private boolean closed = false;

    private boolean joinable = true;

    /**
     * Create any parent directories if necessary, then open the {@link RandomAccessFile} that will receive content on this stream. From that, init
     * the {@link FileChannel} that will be used to write content and map sections of the written file for reading in associated {@link JoinInputStream}
     * instances.
     * <br/>
     * 
     * Initialize the {@link ByteBuffer} that will buffer content before sending it on to the channel (in the {@link #flush()} method).
     */
    public JoinableOutputStream( final File target )
        throws IOException
    {
        this( target, null );
    }

    /**
     * Create any parent directories if necessary, then open the {@link RandomAccessFile} that will receive content on this stream. From that, init
     * the {@link FileChannel} that will be used to write content and map sections of the written file for reading in associated {@link JoinInputStream}
     * instances.
     * <br/>
     * Initialize the {@link ByteBuffer} that will buffer content before sending it on to the channel (in the {@link #flush()} method).
     * <br/>
     * If callbacks are available, use these to signal to a manager instance when the stream is flushed and when
     * the last joined input stream (or this stream, if there are none) closes.
     */
    public JoinableOutputStream( final File target, final StreamCallbacks callbacks )
        throws IOException
    {
        this.file = target;
        this.callbacks = callbacks;

        target.getParentFile()
              .mkdirs();

        randomAccessFile = new RandomAccessFile( target, "rw" );
        channel = randomAccessFile.getChannel();
        buf = ByteBuffer.allocateDirect( CHUNK_SIZE );
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
            throw new IOException( "Joinable output stream in the process of closing. Cannot join!" );
        }

        logger.debug( "JOIN: {}", Thread.currentThread()
                                             .getName() );
        jointCount++;
        return new JoinInputStream();
    }

    /**
     * If the stream is marked as closed, throw {@link IOException}. If the internal buffer is full, call {@link #flush()}. Then, write the byte to 
     * the buffer and increment the written-byte count.
     */
    @Override
    public synchronized void write( final int b )
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
        written++;
    }

    /**
     * Empty the current buffer into the {@link FileChannel} and reinitialize it for filling. Increment the flushed-byte count, which is used as the
     * read limit for associated {@link JoinInputStream}s. Notify anyone listening that there is new content via {@link JoinableOutputStream#notifyAll()}.
     */
    @Override
    public synchronized void flush()
        throws IOException
    {
        //        System.out.println( "FLUSH" );
        buf.flip();
        flushed += channel.write( buf );
        buf.clear();

        super.flush();

        notifyAll();

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
    public synchronized void close()
        throws IOException
    {
        flush();
        closed = true;

        if ( jointCount <= 0 )
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
        if ( callbacks != null )
        {
            callbacks.beforeClose();
        }

        joinable = false;

        channel.close();
        randomAccessFile.close();

        if ( callbacks != null )
        {
            callbacks.closed();
        }
    }

    /**
     * Callback for use in {@link JoinInputStream} to notify this stream to decrement its count of associated input streams. Then, call 
     * {@link #notifyAll()} just in case {@link #close()} is executing, so it can re-check the jointCount and see if it's time to close down
     * the backing storage.
     * @throws IOException 
     */
    private synchronized void jointClosed()
        throws IOException
    {
        jointCount--;
        notifyAll();

        if ( jointCount <= 0 )
        {
            reallyClose();
        }
    }

    /**
     * Retrieve the number of bytes written.
     */
    public long getWritten()
    {
        return written;
    }

    /**
     * Retrieve the number of bytes flushed to the {@link FileChannel} (possibly not yet sync'ed to disk).
     */
    public long getFlushed()
    {
        return flushed;
    }

    /**
     * Retrieve the {@link File} into which content is being written.
     */
    public File getFile()
    {
        return file;
    }

    /**
     * {@link InputStream} associated with a particular {@link JoinableOutputStream} instance. This stream reads content that the output stream has 
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

        /**
         * Map the content already written to disk for reading. If the flushed count exceeds MAX_BUFFER_SIZE, use the max instead.
         */
        JoinInputStream()
            throws IOException
        {
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
                throw new IOException( "Cannot read from closed stream!" );
            }

            synchronized ( JoinableOutputStream.this )
            {
                while ( read == flushed )
                {
                    if ( JoinableOutputStream.this.closed )
                    {
                        // if the parent stream is closed, return EOF
                        return -1;
                    }

                    try
                    {
                        JoinableOutputStream.this.wait( 100 );
                    }
                    catch ( final InterruptedException e )
                    {
                        // if we're interrupted, return EOF
                        return -1;
                    }
                }
            }

            if ( buf.position() == buf.limit() )
            {
                // map more content from the file, reading past our read-bytes count up to the number of flushed bytes from the parent stream
                buf = channel.map( MapMode.READ_ONLY, read, flushed - read );
            }

            // be extra careful...if the new buffer is empty, return EOF.
            if ( buf.position() == buf.limit() )
            {
                return -1;
            }

            final int result = buf.get();
            read++;

            return result;
        }

        /**
         * Mark this stream as closed to no further reads can proceed. Then, call {@link JoinableOutputStream#jointClosed()} to notify the parent
         * output stream to decrement its open-reader count and notify anyone waiting in case a close is in progress.
         */
        @Override
        public void close()
            throws IOException
        {
            closed = true;
            super.close();
            jointClosed();
        }
    }

}
