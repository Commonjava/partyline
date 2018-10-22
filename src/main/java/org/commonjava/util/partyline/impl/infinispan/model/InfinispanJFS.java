package org.commonjava.util.partyline.impl.infinispan.model;

import org.commonjava.util.partyline.callback.StreamCallbacks;
import org.commonjava.util.partyline.lock.local.LocalLockManager;
import org.commonjava.util.partyline.lock.local.LocalLockOwner;
import org.commonjava.util.partyline.lock.local.ReentrantOperationLock;
import org.commonjava.util.partyline.spi.JoinableFile;
import org.commonjava.util.partyline.spi.JoinableFilesystem;
import org.infinispan.Cache;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

public class InfinispanJFS
                implements JoinableFilesystem
{
    private final LocalLockManager lockManager = new LocalLockManager();

    private String nodeKey;

    private final Cache<String, FileMeta> metadataCache;

    private final Cache<UUID, FileBlock> blockCache;

    public InfinispanJFS( final String nodeKey, final Cache<String, FileMeta> metadataCache,
                          final Cache<UUID, FileBlock> blockCache )
    {
        this.nodeKey = nodeKey;
        this.metadataCache = metadataCache;
        this.blockCache = blockCache;
    }

    @Override
    public JoinableFile getFile( final File file, final LocalLockOwner lockOwner, final StreamCallbacks callbacks,
                                 final boolean doOutput, final ReentrantOperationLock opLock ) throws IOException
    {
        return new InfinispanJF( file, lockOwner, callbacks, doOutput, opLock, this );
    }

    @Override
    public LocalLockManager getLocalLockManager()
    {
        return null;
    }

    FileBlock getNextBlock( final FileBlock prevBlock ) throws InterruptedException
    {
        UUID next = prevBlock.getNextBlockID();
        FileBlock nextBlock = blockCache.get( next );
        if ( nextBlock == null )
        {
            // setup a cache listener for this UUID, and wait in a timed loop for it to return
        }

        return nextBlock;
    }

    void pushNextBlock( final FileBlock prevBlock, final FileBlock nextBlock, final FileMeta metadata )
                    throws IOException
    {
        try
        {
            lockManager.reentrantSynchronous( metadata.getFilePath(), ( opLock ) -> {
                blockCache.put( prevBlock.getBlockID(), prevBlock );
                blockCache.put( nextBlock.getBlockID(), nextBlock );
                metadataCache.put( metadata.getFilePath(), metadata );

                return null;
            } );
        }
        catch ( InterruptedException e )
        {
            throw new IOException( "Thread interrupted while adding next block to file: " + metadata.getFilePath(), e );
        }
    }

    void updateBlock( final FileBlock block ) throws IOException
    {
        blockCache.put( block.getBlockID(), block );
    }

    void close( FileMeta metadata ) throws IOException
    {
        try
        {
            lockManager.reentrantSynchronous( metadata.getFilePath(), ( opLock ) -> {

                // called from InfinispanJF.reallyClose()? I think.
                // if the local lock owner is empty / lock count == 0 for this file, remove it from the node-level locks too.
                // if the local lock owner's count is NOT 0, then we need to make sure our node's lock-level matches the one in
                // the local lock manager, and update if necessary
                LocalLockOwner owner = metadata.getLockOwner();
                if ( owner == null || owner.getContextLockCount() == 0 )
                {
                    // Remove it from node level locks
                }
                else
                {
                    // Compare owner.getLockLevel() with node level lock and update
                }

                return null;
            } );
        }
        catch ( InterruptedException e )
        {
            throw new IOException( "Thread interrupted while closing file: " + metadata.getFilePath(), e );
        }
    }

    FileMeta getMetadata( final File target, final LocalLockOwner owner ) throws IOException
    {
        String path = target.getAbsolutePath();
        try
        {
            return lockManager.reentrantSynchronous( path, ( opLock ) -> {

                // we need to use cache.getAdvancedCache().getTransactionManager() to lock this entry and then update the node-level locks.
                // this will be where we reserve the lock level (or change it, if our node is already represented)
                FileMeta meta = metadataCache.computeIfAbsent( path, ( p ) -> new FileMeta( p, target.isDirectory(),
                                                                                            owner ) );

                return meta;
            } );
        }
        catch ( InterruptedException e )
        {
            throw new IOException( "Thread interrupted while retrieving / creating file metadata : " + path, e );
        }
    }
}
