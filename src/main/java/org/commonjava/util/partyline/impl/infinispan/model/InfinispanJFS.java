package org.commonjava.util.partyline.impl.infinispan.model;

import com.sun.tools.javac.util.Context;
import org.commonjava.util.partyline.callback.StreamCallbacks;
import org.commonjava.util.partyline.lock.LockLevel;
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

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class InfinispanJFS
                implements JoinableFilesystem
{
    private final LocalLockManager lockManager = new LocalLockManager();

    private static final long WAIT_TIMEOUT = 100;

    private String nodeKey;

    private final Cache<String, FileMeta> metadataCache;

    private final Cache<String, FileBlock> blockCache;

    public InfinispanJFS( final String nodeKey, final Cache<String, FileMeta> metadataCache,
                          final Cache<String, FileBlock> blockCache )
    {
        this.nodeKey = nodeKey;
        this.metadataCache = metadataCache;
        this.blockCache = blockCache;
        this.nodeKey = nodeKey;
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
        return lockManager;
    }

    @Override
    public void updateDominantLocks( String path, LockLevel level )
                    throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException,
                    RollbackException
    {
        TransactionManager transactionManager = metadataCache.getAdvancedCache().getTransactionManager();
        transactionManager.begin();
        FileMeta meta = metadataCache.get( path );
        if ( level == null )
        {
            meta.removeLock( this.nodeKey );
        }
        else
        {
            meta.addLock( this.nodeKey, level );
        }
        metadataCache.put( path, meta );
        transactionManager.commit();
    }

    FileBlock getNextBlock( final FileBlock prevBlock, final FileMeta metadata ) throws IOException
    {
        String next = prevBlock.getNextBlockID();
        if ( next == null )
        {
            return null;
        }
        // setup a cache listener for the ID, and wait in a timed loop for it to return
        try
        {
            lockManager.reentrantSynchronous( metadata.getFilePath(), ( opLock ) -> {
                FileBlock nextBlock = null;
                ClusterListener clusterListener = new ClusterListener( next, opLock );
                while ( nextBlock == null )
                {
                    nextBlock = blockCache.get( next );
                    if ( nextBlock == null )
                    {
                        clusterListener.listenToCache( blockCache );
                    }
                }
                return nextBlock;
            } );
        }
        catch ( InterruptedException e )
        {
            e.printStackTrace();
        }
        return null;
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

    void close( FileMeta metadata, LocalLockOwner owner ) throws IOException
    {

        TransactionManager transactionManager = metadataCache.getAdvancedCache().getTransactionManager();
        try
        {
            lockManager.reentrantSynchronous( metadata.getFilePath(), ( opLock ) -> {

                // called from InfinispanJF.reallyClose()? I think.
                // if the local lock owner is empty / lock count == 0 for this file, remove it from the node-level locks too.
                // if the local lock owner's count is NOT 0, then we need to make sure our node's lock-level matches the one in
                // the local lock manager, and update if necessary
                if ( owner == null || owner.getContextLockCount() == 0 )
                {
                    metadata.removeLock( this.nodeKey );
                    transactionManager.begin();
                    metadataCache.put( metadata.getFilePath(), metadata );
                    transactionManager.commit();
                }
                else
                {
                    // Compare owner.getLockLevel() with nodeLevel lock and update
                    LockLevel nodeLevel = metadata.getLockLevel( this.nodeKey );
                    // TODO: Negotiate the lock level

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

                TransactionManager transactionManager = metadataCache.getAdvancedCache().getTransactionManager();
                transactionManager.begin();
                FileMeta meta = metadataCache.computeIfAbsent( path, ( p ) -> new FileMeta( p, target.isDirectory() ) );
                // Check the current lock level
                // TODO: Negotiate the lock level
                LockLevel currentLockLevel = meta.getLockLevel( this.nodeKey );

                meta.addLock( this.nodeKey, owner.getLockLevel() );
                metadataCache.put( path, meta );
                transactionManager.commit();

                return meta;
            } );
        }
        catch ( InterruptedException e )
        {
            e.printStackTrace();
        }
        return null;
    }

    @Listener
    public class ClusterListener
    {
        List<CacheEntryEvent> events = Collections.synchronizedList( new ArrayList<CacheEntryEvent>() );

        String key;

        ReentrantOperationLock lock;

        public ClusterListener( String key, ReentrantOperationLock opLock )
        {
            this.key = key;
            this.lock = opLock;
        }

        @CacheEntryCreated
        public void onCacheCreatedEvent( CacheEntryEvent event )
        {
            // Check to see if the new entry is the one we're listening for
            if ( event.getKey().equals( this.key ) )
            {
                this.lock.signal();
            }
            events.add( event );
        }

        @CacheEntryModified
        public void onCacheEventModified( CacheEntryEvent event )
        {
            if ( event.getKey().equals( this.key ) )
            {
                // Check to see if updated block was marked EOF
                FileBlock updatedBlock = (FileBlock) event.getValue();
                if ( updatedBlock.isEOF() )
                {
                    this.lock.signal();
                }
            }
        }

        public void listenToCache( Cache<?, ?> cache ) throws IOException
        {
            cache.addListener( this );

            try
            {
                this.lock.await( WAIT_TIMEOUT );
            }
            catch ( InterruptedException e )
            {
                throw new IOException( "Thread interrupted while retrieving / creating file metadata" );
            }
        }
    }

}
