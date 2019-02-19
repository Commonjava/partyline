package org.commonjava.util.partyline.impl.infinispan;

import org.commonjava.util.partyline.PartylineException;
import org.commonjava.util.partyline.callback.StreamCallbacks;
import org.commonjava.util.partyline.impl.infinispan.model.FileBlock;
import org.commonjava.util.partyline.impl.infinispan.model.FileMeta;
import org.commonjava.util.partyline.lock.LockLevel;
import org.commonjava.util.partyline.lock.UnlockStatus;
import org.commonjava.util.partyline.lock.local.LocalLockManager;
import org.commonjava.util.partyline.lock.local.LocalLockOwner;
import org.commonjava.util.partyline.lock.local.ReentrantOperationLock;
import org.commonjava.util.partyline.spi.JoinableFile;
import org.commonjava.util.partyline.spi.JoinableFilesystem;
import org.infinispan.Cache;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.slf4j.LoggerFactory;

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

    private final int blockSize;

    private static final int DEFAULT_BLOCK_SIZE = 1024 * 1024 * 8; // 8mb

    public InfinispanJFS( final String nodeKey, final Cache<String, FileMeta> metadataCache,
                          final Cache<String, FileBlock> blockCache )
    {
        this( nodeKey, metadataCache, blockCache, DEFAULT_BLOCK_SIZE );
    }

    public InfinispanJFS( final String nodeKey, final Cache<String, FileMeta> metadataCache,
                          final Cache<String, FileBlock> blockCache, int blockSize )
    {
        this.nodeKey = nodeKey;
        this.metadataCache = metadataCache;
        this.blockCache = blockCache;
        this.nodeKey = nodeKey;
        this.blockSize = blockSize;
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
    public void updateDominantLocks( String path, UnlockStatus unlockStatus ) throws PartylineException
    {
        // Do nothing if dominance did not change
        if ( !unlockStatus.isDominanceChanged() )
        {
            return;
        }
        TransactionManager transactionManager = metadataCache.getAdvancedCache().getTransactionManager();
        try
        {
            transactionManager.begin();
            FileMeta meta = metadataCache.get( path );
            if ( unlockStatus.getDominantLockLevel() == null )
            {
                meta.removeLock( this.nodeKey );
            }
            else
            {
                meta.setLock( this.nodeKey, unlockStatus.getDominantLockLevel() );
            }
            metadataCache.put( path, meta );
        }
        catch ( NotSupportedException | SystemException e )
        {
            try
            {
                transactionManager.rollback();
                throw new PartylineException( "Failed to begin transaction. Rolling back. Path: " + path, e );
            }
            catch ( SystemException e1 )
            {
                LoggerFactory.getLogger( getClass().getName() )
                             .error( "System Exception during transaction rollback involving path: " + path, e1 );
            }
        }
        finally
        {
            try
            {
                transactionManager.commit();
            }
            catch ( RollbackException | HeuristicMixedException | HeuristicRollbackException | SystemException e )
            {
                LoggerFactory.getLogger( getClass().getName() )
                             .error( "Exception during transaction commit involving path: " + path, e );
            }
        }
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
            return lockManager.reentrantSynchronous( metadata.getFilePath(), ( opLock ) -> {
                ClusterListener clusterListener = new ClusterListener( next, opLock );
                FileBlock nextBlock = null;
                while ( nextBlock == null )
                {
                    nextBlock = blockCache.get( next );
                    if ( nextBlock == null )
                    {
                        clusterListener.listenToCacheAndWait( blockCache );
                    }
                }
                return nextBlock;
            } );
        }
        catch ( InterruptedException e )
        {
            LoggerFactory.getLogger( getClass().getName() )
                         .error( "Interrupted while trying to get block with ID: " + next, e );
        }
        return null;
    }

    void pushNextBlock( final FileBlock prevBlock, final FileBlock nextBlock, final FileMeta metadata )
                    throws IOException
    {
        try
        {
            lockManager.reentrantSynchronous( metadata.getFilePath(), ( opLock ) -> {
                updateBlock( prevBlock );
                // If prevBlock is EOF then there will be no nextBlock
                if ( nextBlock != null )
                {
                    updateBlock( nextBlock );
                }

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
        TransactionManager transactionManager = blockCache.getAdvancedCache().getTransactionManager();
        try
        {
            transactionManager.begin();
            blockCache.put( block.getBlockID(), block );
        }
        catch ( NotSupportedException | SystemException e )
        {
            try
            {
                transactionManager.rollback();
                throw new PartylineException( "Failed to begin transaction. Rolling back. Block: " + block.getBlockID(),
                                              e );
            }
            catch ( SystemException e1 )
            {
                LoggerFactory.getLogger( getClass().getName() )
                             .error( "System Exception during transaction rollback involving Block: "
                                                     + block.getBlockID(), e1 );
            }
        }
        finally
        {

            try
            {
                transactionManager.commit();
            }
            catch ( RollbackException | HeuristicMixedException | HeuristicRollbackException | SystemException e )
            {
                LoggerFactory.getLogger( getClass().getName() )
                             .error( "Exception during transaction commit involving block: " + block.getBlockID(), e );
            }

        }

    }

    void close( FileMeta metadata, LocalLockOwner owner ) throws IOException
    {
        /*
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
        }*/
    }

    FileMeta getMetadata( final File target, final LocalLockOwner owner ) throws IOException
    {
        String path = target.getAbsolutePath();
        try
        {
            return lockManager.reentrantSynchronous( path, ( opLock ) -> {

                FileMeta meta = null;
                TransactionManager transactionManager = metadataCache.getAdvancedCache().getTransactionManager();

                try
                {
                    transactionManager.begin();

                    meta = metadataCache.computeIfAbsent( path, ( p ) -> new FileMeta( p, target.isDirectory(),
                                                                                       this.blockSize ) );

                    LockLevel currentLockLevel = meta.getLockLevel( this.nodeKey );
                    // Only update the cache if the lock level changed
                    if ( currentLockLevel == null || currentLockLevel != owner.getLockLevel() )
                    {
                        meta.setLock( this.nodeKey, owner.getLockLevel() );
                        metadataCache.put( path, meta );
                    }

                }
                catch ( NotSupportedException | SystemException e )
                {
                    try
                    {
                        transactionManager.rollback();
                        throw new PartylineException( "Failed to begin transaction. Rolling back. Path: " + path, e );
                    }
                    catch ( SystemException e1 )
                    {
                        LoggerFactory.getLogger( getClass().getName() )
                                     .error( "System Exception during transaction rollback involving path: " + path,
                                             e1 );
                    }
                }
                finally
                {
                    try
                    {
                        transactionManager.commit();
                    }
                    catch ( RollbackException | HeuristicMixedException | HeuristicRollbackException | SystemException e )
                    {
                        LoggerFactory.getLogger( getClass().getName() )
                                     .error( "Exception during transaction commit involving path: " + path, e );
                    }

                }
                return meta;
            } );
        }
        catch ( InterruptedException e )
        {
            LoggerFactory.getLogger( getClass().getName() ).error( "Problem retrieving metadata for path: " + path, e );
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

        public void listenToCacheAndWait( Cache<?, ?> cache ) throws IOException
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
