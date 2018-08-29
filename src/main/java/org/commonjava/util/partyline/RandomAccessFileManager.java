package org.commonjava.util.partyline;

import org.commonjava.util.partyline.FileInterface;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;
import java.io.Closeable;


public class RandomAccessFileManager
        implements FileInterface, AutoCloseable, Closeable
{

    private final FileChannel channel;
    private final RandomAccessFile randomAccessFile;

    public RandomAccessFileManager( File target, String mode )
    {
        randomAccessFile = new RandomAccessFile( target, mode );
        channel = randomAccessFile.getChannel();
    }

    @Override
    public void close()
        throws IOException
    {
    }

    @Override
    public int write( ByteBuffer buf )
        throws IOException
    {
        int count = 0;

        while ( buf.hasRemaining() )
        {
            count += channel.write( buf );
        }
        channel.force( true );
        return count;
    }

    @Override
    public int read()
        throws IOException
    {

    }

}