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
package org.commonjava.util.partyline.impl.infinispan;

import org.commonjava.util.partyline.callback.StreamCallbacks;
import org.commonjava.util.partyline.impl.infinispan.model.FileBlock;
import org.commonjava.util.partyline.impl.infinispan.model.FileMeta;
import org.commonjava.util.partyline.lock.UnlockStatus;
import org.commonjava.util.partyline.lock.local.LocalLockOwner;
import org.commonjava.util.partyline.lock.local.ReentrantOperation;
import org.commonjava.util.partyline.lock.local.ReentrantOperationLock;
import org.commonjava.util.partyline.spi.JoinableFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.OverlappingFileLockException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.UUID;

public final class InfinispanJF
                implements AutoCloseable, Closeable, JoinableFile
{
    private static final int CHUNK_SIZE = 1024 * 1024; // 1mb

    private final JoinableOutputStream output;

    private final Map<Integer, JoinInputStream> inputs = new ConcurrentHashMap<>();

    private final String path;

    private final InfinispanJFS filesystem;

    private final FileMeta metadata;

    private FileBlock prevBlock;

    private FileBlock currBlock;

    private final StreamCallbacks callbacks;

    private volatile boolean closed = false;

    private volatile boolean joinable = true;

    private final LocalLockOwner owner;

    private final ReentrantOperationLock opLock;

    InfinispanJF( final File target, final LocalLockOwner owner, final StreamCallbacks callbacks, boolean doOutput,
                  ReentrantOperationLock opLock, InfinispanJFS filesystem ) throws IOException
    {
        this.owner = owner;
        this.path = target.getPath();
        this.filesystem = filesystem;
        this.callbacks = callbacks;
        this.opLock = opLock;

        target.getParentFile().mkdirs();

        Logger logger = LoggerFactory.getLogger( getClass() );
        logger.trace( "Trying to initialize JoinableFile to: {} using operation lock:\n\n{}", target, opLock );
        try
        {
            this.metadata = filesystem.getMetadata( target, owner );
            this.currBlock = metadata.getFirstBlock();
            if ( this.currBlock == null )
            {
                currBlock = this.metadata.createBlock( UUID.randomUUID(), true );
            }

            if ( target.isDirectory() )
            {
                logger.trace( "INIT: locking directory WITHOUT lock in underlying filesystem!" );
                output = null;
                //                fileLock = null;
                joinable = false;
            }
            else if ( doOutput )
            {
                logger.trace( "INIT: read-write JoinableFile: {}", target );
                output = new JoinableOutputStream();
            }
            else
            {
                logger.trace( "INIT: read-only JoinableFile: {}", target );
                output = null;
            }
        }
        catch ( OverlappingFileLockException e )
        {
            throw new IOException( "Cannot lock file: " + target + ". Reason: " + e.getMessage() + "\nLocked by: "
                                                   + owner.getLockInfo(), e );
        }
    }

    public LocalLockOwner getLockOwner()
    {
        return owner;
    }

    // only public for testing purposes...
    public OutputStream getOutputStream()
    {
        return output;
    }

    public boolean isJoinable()
    {
        return joinable;
    }

    @Override
    public boolean isDirectory()
    {
        return metadata.isDirectory();
    }

    public InputStream joinStream() throws IOException, InterruptedException
    {
        return lockAnd( ( lock ) -> {
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
        } );
    }

    @Override
    public boolean isWriteLocked()
    {
        return metadata.isDirectory() || output != null;
    }

    private <T> T lockAnd( ReentrantOperation<T> op ) throws IOException, InterruptedException
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
    public void close() throws IOException
    {
        try
        {
            lockAnd( ( lock ) -> {
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
    private void reallyClose() throws IOException
    {
        Logger logger = LoggerFactory.getLogger( getClass() );
        logger.trace( "Really closing InfinispanJF: {}", path );

        try
        {
            lockAnd( ( lock ) -> {

                if ( callbacks != null )
                {
                    logger.trace( "calling beforeClose() on callbacks: {}", callbacks );
                    callbacks.beforeClose();
                }

                joinable = false;

                if ( output != null )
                {
                    filesystem.close( metadata, owner );

                }
                logger.trace( "InfinispanJF for: {} is really closed (by thread: {}).", path,
                              Thread.currentThread().getName() );

                if ( callbacks != null )
                {
                    logger.trace( "calling closed() on callbacks: {}", callbacks );
                    callbacks.closed();
                }

                return null;
            } );
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
    private void jointClosed( JoinInputStream input, String originalThreadName ) throws IOException
    {
        try
        {
            lockAnd( ( lock ) -> {
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
                    UnlockStatus unlockStatus = owner.unlock( labelFor( false, originalThreadName ) );
                    filesystem.updateDominantLocks( this.path, unlockStatus );
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
    public String getPath()
    {
        return path;
    }

    public boolean isOpen()
    {
        return !closed || !inputs.isEmpty();
    }

    public String reportOwnership()
    {
        StringBuilder sb = new StringBuilder();
        sb.append( "Path: " ).append( path );

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
        inputs.forEach( ( hashCode, instance ) -> sb.append( "\n\t- " ).append( instance.reportWithOwner() ) );

        return sb.toString();
    }

    public static String labelFor( final boolean doOutput, String threadName )
    {
        return ( doOutput ? "WRITE via " : "READ via " ) + threadName;
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
        public void write( final int b ) throws IOException
        {
            if ( closed )
            {
                throw new IOException( "Cannot write to closed stream!" );
            }

            if ( (int) b == -1 )
            {
                currBlock.setEOF();
                return;
            }

            if ( currBlock.full() )
            {
                flush();
            }

            currBlock.writeToBuffer( (byte) b );
        }

        public void write( ByteBuffer buf ) throws IOException
        {
            while ( buf.hasRemaining() )
            {
                byte b = buf.get();
                write( (int) b );
            }
        }

        @Override
        public void flush() throws IOException
        {
            synchronized ( InfinispanJF.this )
            {
                if ( closed )
                {
                    throw new IOException( "Cannot write to closed stream!" );
                }
            }

            doFlush( false );
        }

        @Override
        public void close() throws IOException
        {
            Logger logger = LoggerFactory.getLogger( getClass() );
            logger.trace( "OUT ({}):: close() called", originalThreadName );

            if ( closed )
            {
                logger.trace( "OUT ({}):: already closed", originalThreadName );
                return;
            }
            doFlush( true );
            closed = true;
            super.close();

            InfinispanJF.this.close();
        }

        public void doFlush( boolean eof ) throws IOException
        {
            if ( eof )
            {
                // Update the block pointers
                prevBlock = currBlock;
                currBlock = null;
                prevBlock.getBuffer().flip();
                filesystem.pushNextBlock( prevBlock, currBlock, metadata );
            }
            else
            {
                // Update the block pointers
                String newBlockID = UUID.randomUUID().toString();
                FileBlock block = new FileBlock( metadata.getFilePath(), newBlockID, metadata.getBlockSize() );
                currBlock.setNextBlockID( newBlockID );
                prevBlock = currBlock;
                prevBlock.getBuffer().flip();
                currBlock = block;
                filesystem.updateBlock( prevBlock );
            }

            synchronized ( InfinispanJF.this )
            {
                InfinispanJF.this.notifyAll();
            }

            if ( callbacks != null )
            {
                callbacks.flushed();
            }
        }

        boolean isClosed()
        {
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
        JoinInputStream( int jointIdx ) throws IOException
        {
            this.jointIdx = jointIdx;
            block = metadata.getFirstBlock();
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
        public int read() throws IOException
        {
            synchronized ( InfinispanJF.this )
            {
                if ( closed )
                {
                    throw new IOException( "Joint: " + jointIdx + "(" + originalThreadName
                                                           + "): Cannot read from closed stream!" );
                }

            }
            if ( block.hasRemaining() )
            {
                // We're done reading the buffer - check for EOF
                if ( block.isEOF() )
                {
                    return -1;
                }
                try
                {
                    // Get the next block
                    FileBlock next = filesystem.getNextBlock( block, metadata );
                    block = next;
                    // getNextBlock can return null
                    if ( block == null )
                    {
                        return -1;
                    }
                }
                catch ( final IOException e )
                {
                    return -1;
                }

            }

            final int result = block.readFromBuffer();

            return result;
        }

        /**
         * Mark this stream as closed to no further reads can proceed. Then, call {@link InfinispanJF#jointClosed(JoinInputStream, String)} to notify the parent
         * output stream to decrement its open-reader count and notify anyone waiting in case a close is in progress.
         */
        @Override
        public void close() throws IOException
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
        return "JoinableFile{" + "path='" + path + '\'' + '}';
    }
}
