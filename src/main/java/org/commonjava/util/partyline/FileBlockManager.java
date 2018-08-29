package org.commonjava.util.partyline;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.commonjava.util.partyline.FileBlock;
import org.commonjava.util.partyline.FileInterface;
import org.commonjava.util.partyline.FileMeta;

import java.util.UUID;



public class FileBlockManager
        implements FileInterface 
{

    private final DefaultCacheManager fileCache;

    private final DefaultCacheManager blockCache;

    private FileMeta fileMeta = null;

    private FileBlock prevBlock = null;

    private FileBlock currBlock = null;

    public FileBlockManager( File target )
    {

    }

    @Override
    public void flush()
    {
        doFlush( false );
    }

    public void close()
    {
        doFlush( true );
    }

    public void doFlush( boolean eof )
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
    }

    @Override
    public int write( ByteBuffer buf)
        throws IOException
    {
        int count = 0;
        while ( buf.hasRemaining() )
        {
            byte b = buf.get();
            //check for case where b is eof
            if ( (int) b == -1 )
            {
                currBlock.setEOF();
                doFlush( true );
                return -1;
            }
            if ( !currBlock.full() )
            {
                currBlock.writeToBuffer( b );
                count ++;
            }
            else
            {
                UUID newBlockID = UUID.randomUUID();
                FileBlock block = new FileBlock( fileMeta.UUID, newBlockID );
                currBlock.setNextBlockID( newBlockID );
                prevBlock = currBlock;
                currBlock = block;

                flush();

                currBlock.writeToBuffer ( b );
                count ++;
            }
        }
        return count;
    }

    public int read()
        throws IOException
    {
        
    }
}