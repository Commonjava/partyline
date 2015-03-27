package org.commonjava.util.partyline;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

public class JoinableOutputStream
    extends OutputStream
{
    private static final int CHUNK_SIZE = 1024 * 1024; // 1 mb

    private final FileChannel channel;

    private boolean closed = false;

    private long written = 0;

    private long flushed = 0;

    private ByteBuffer buf;

    private int jointCount = 0;

    private final File file;

    private final RandomAccessFile randomAccessFile;

    public JoinableOutputStream( final File target )
        throws IOException
    {
        this.file = target;

        target.getParentFile()
              .mkdirs();

        randomAccessFile = new RandomAccessFile( target, "rw" );
        channel = randomAccessFile.getChannel();
        buf = ByteBuffer.allocateDirect( CHUNK_SIZE );
    }

    public InputStream joinStream()
        throws IOException
    {
        if ( closed )
        {
            throw new IOException( "Cannot join closed IO!" );
        }

        System.out.println( "JOIN: " + Thread.currentThread()
                                            .getName() );
        jointCount++;
        return new JoinInputStream();
    }

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

    @Override
    public synchronized void flush()
        throws IOException
    {
        System.out.println( "FLUSH" );
        final ByteBuffer toWrite = buf;
        buf = ByteBuffer.allocateDirect( CHUNK_SIZE );

        toWrite.flip();
        flushed += channel.write( toWrite );
        super.flush();

        notifyAll();
    }

    @Override
    public synchronized void close()
        throws IOException
    {
        flush();
        closed = true;

        while ( jointCount > 0 )
        {
            try
            {
                wait( 100 );
            }
            catch ( final InterruptedException e )
            {
                break;
            }
        }

        reallyClose();
    }

    private void reallyClose()
        throws IOException
    {
        channel.close();
        randomAccessFile.close();
    }

    private synchronized void jointClosed()
    {
        jointCount--;
        notifyAll();
    }

    public long getWritten()
    {
        return written;
    }

    public long getFlushed()
    {
        return flushed;
    }

    public File getFile()
    {
        return file;
    }

    private final class JoinInputStream
        extends InputStream
    {
        private long read = 0;

        private ByteBuffer buf;

        private boolean closed = false;

        JoinInputStream()
            throws IOException
        {
            buf = channel.map( MapMode.READ_ONLY, 0, flushed );
        }

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
                        return -1;
                    }

                    try
                    {
                        JoinableOutputStream.this.wait( 100 );
                    }
                    catch ( final InterruptedException e )
                    {
                        return -1;
                    }
                }
            }

            if ( buf.position() == buf.limit() )
            {
                buf = channel.map( MapMode.READ_ONLY, read, flushed - read );
            }

            final int result = buf.get();
            read++;

            return result;
        }

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
