package org.commonjava.util.partyline.impl.infinispan.model;

import com.sun.tools.javac.util.Context;
import org.commonjava.util.partyline.callback.StreamCallbacks;
import org.commonjava.util.partyline.lock.local.LocalLockManager;
import org.commonjava.util.partyline.lock.local.LocalLockOwner;
import org.commonjava.util.partyline.lock.local.ReentrantOperation;
import org.commonjava.util.partyline.lock.local.ReentrantOperationLock;
import org.commonjava.util.partyline.spi.JoinableFile;
import org.commonjava.util.partyline.spi.JoinableFilesystem;
import org.infinispan.Cache;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class InfinispanJFS
                implements JoinableFilesystem
{
    private final LocalLockManager lockManager = new LocalLockManager();

    private static final long WAIT_TIMEOUT = 100;

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

    FileBlock getNextBlock( final FileBlock prevBlock, final FileMeta metadata) throws IOException
    {
        synchronized ( InfinispanJFS.this )
        {
            // TODO: make sure next will exist
            UUID next = prevBlock.getNextBlockID();
            FileBlock nextBlock = null;
            while ( nextBlock == null )
            {
                nextBlock = blockCache.get( next );
                if (nextBlock == null)
                {
                    try
                    {
                        lockManager.reentrantSynchronous( metadata.getFilePath(), ( opLock ) -> {
                            // setup a cache listener for this UUID, and wait in a timed loop for it to return
                            ClusterListener clusterListener = new ClusterListener( next, opLock );
                            clusterListener.listenToCache( blockCache );
                            opLock.await( WAIT_TIMEOUT );

                            return null;
                        } );
                    }
                    catch ( final InterruptedException e )
                    {
                        return null;
                    }
                }

            }

            return nextBlock;
        }
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

    @Listener
    public class ClusterListener
    {
        List<CacheEntryEvent> events = Collections.synchronizedList( new ArrayList<CacheEntryEvent>() );

        UUID key;
        ReentrantOperationLock lock;

        public ClusterListener( UUID key, ReentrantOperationLock opLock )
        {
            this.key = key;
            this.lock = opLock;
        }

        @CacheEntryCreated
        public void onCacheCreatedEvent( CacheEntryEvent event )
        {
            // Check to see if the new entry is the one we're listening for
            if ( event.getKey() == this.key )
            {
                // TODO: Do something here to notify
                this.lock.signal();
            }
            events.add( event );
        }

        @CacheEntryModified
        public void onCacheEventModified( CacheEntryEvent event )
        {
            if ( event.getKey() == this.key )
            {
                // Check to see if updated block was marked EOF
                FileBlock updatedBlock = (FileBlock) event.getValue();
                if( updatedBlock.isEOF() )
                {
                    this.lock.signal();
                }
            }
        }

        public void listenToCache( Cache<?, ?> cache )
        {
            cache.addListener( this );
        }
    }

}
