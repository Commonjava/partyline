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

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.commonjava.util.partyline.callback.StreamCallbacks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.commonjava.util.partyline.LockLevel.read;

/**
 * Created by jdcasey on 8/18/16.
 */
final class FileTree
{

    private static final long DEFAULT_LOCK_TIMEOUT = 5000;

    private static final long WAIT_TIMEOUT = 100;

    private Map<String, FileEntry> entryMap = new WeakHashMap<>();

    private Map<String, FileOperationLock> operationLocks = new WeakHashMap<>();

    void forAll( String operationName, Consumer<JoinableFile> fileConsumer )
    {
        TreeMap<String, FileEntry> sorted = new TreeMap<>( entryMap );
        sorted.forEach( ( key, entry ) -> {
            if ( entry.file != null )
            {
                fileConsumer.accept( entry.file );
            }
        } );
    }

    void forFilesOwnedBy( long ownerId, String operationName, Function<JoinableFile, Boolean> fileConsumer )
    {
        TreeMap<String, FileEntry> sorted = new TreeMap<>( entryMap );
        AtomicBoolean continueProcessing = new AtomicBoolean( true );
        sorted.forEach( ( key, entry ) -> {
            if ( continueProcessing.get() && entry.file != null && entry.lock.getThreadId() == ownerId )
            {
                if ( !fileConsumer.apply( entry.file ) )
                {
                    continueProcessing.set( false );
                }
            }
        } );
    }

    String renderTree()
    {
        StringBuilder sb = new StringBuilder();
        TreeMap<String, FileEntry> sorted = new TreeMap<>( entryMap );
        sorted.forEach( ( key, entry ) -> {
            sb.append( "+- " );
            Stream.of( key.split( "/" ) ).forEach( ( part ) -> sb.append( "  " ) );

            sb.append( new File( key ).getName() );
            if ( entry.file != null )
            {
                sb.append( " (F)" );
            }
            else
            {
                sb.append( "/" );
            }
        } );
        return sb.toString();
    }

    LockLevel getLockLevel( File file )
    {
        FileEntry entry = getLockingEntry( file );
        Logger logger = LoggerFactory.getLogger( getClass() );
        if ( entry == null )
        {
            return null;
        }
        else if ( !entry.name.equals( file.getAbsolutePath() ) )
        {
            return read;
        }
        else
        {
            return entry.lock.getLockLevel();
        }
    }

    boolean unlock( File f, String ownerName )
    {
        try
        {
            return withOpLock( f, ( opLock ) -> {
                Logger logger = LoggerFactory.getLogger( getClass() );
                FileEntry entry = entryMap.get( f.getAbsolutePath() );
                if ( entry != null )
                {
                    //            synchronized ( entry )
                    //            {
                    logger.trace( "Unlocking {} (owner: {})", f, ownerName );
                    if ( entry.lock.unlock( ownerName ) )
                    {
                        logger.trace( "Unlocked; clearing resources associated with lock" );
                        if ( entry.file != null )
                        {
                            logger.trace( "{} Closing file...", ownerName );
                            IOUtils.closeQuietly( entry.file );
                            entry.file = null;
                        }

                        entryMap.remove( entry.name );

                        //                    entry.notifyAll();
                        opLock.signal();
                        logger.trace( "Unlock succeeded." );
                        return true;
                    }
                    else
                    {
                        logger.trace( "{} Unlock request failed: Remaining locks:\n\n{}", ownerName, entry.lock.getLockInfo() );
                    }
                    //            }
                }
                else
                {
                    logger.trace( "{} not locked by {}", f, ownerName );
                }

                opLock.signal();
                logger.trace( "Unlock failed." );
                return false;
            } );
        }
        catch ( IOException e )
        {
            Logger logger = LoggerFactory.getLogger( getClass() );
            logger.error( "SHOULD NEVER HAPPEN: IOException trying to unlock: " + f, e );
        }
        catch ( InterruptedException e )
        {
            Logger logger = LoggerFactory.getLogger( getClass() );
            logger.warn( "Interrupted while trying to unlock: " + f );
        }

        return false;
    }

    private boolean clearLocks( File f )
    {
        try
        {
            return withOpLock( f, ( opLock ) -> {
                Logger logger = LoggerFactory.getLogger( getClass() );
                FileEntry entry = entryMap.get( f.getAbsolutePath() );
                if ( entry != null )
                {
                    //            synchronized ( entry )
                    //            {
                    logger.trace( "Unlocking {}", f );
                    entry.lock.clearLocks();
                    logger.trace( "Unlocked; clearing resources associated with lock" );
                    if ( entry.file != null )
                    {
                        logger.trace( "Closing file..." );
                        IOUtils.closeQuietly( entry.file );
                        entry.file = null;
                    }

                    entryMap.remove( entry.name );

                    //                    entry.notifyAll();
                    opLock.signal();
                    logger.trace( "Unlock succeeded." );
                    return true;
                    //            }
                }
                else
                {
                    logger.trace( "{} not locked", f );
                }

                opLock.signal();
                logger.trace( "Unlock failed." );
                return false;
            } );
        }
        catch ( IOException e )
        {
            Logger logger = LoggerFactory.getLogger( getClass() );
            logger.error( "SHOULD NEVER HAPPEN: IOException trying to unlock: " + f, e );
        }
        catch ( InterruptedException e )
        {
            Logger logger = LoggerFactory.getLogger( getClass() );
            logger.warn( "Interrupted while trying to unlock: " + f );
        }

        return false;
    }

    boolean tryLock( File file, String ownerName, String label, LockLevel lockLevel, long timeout, TimeUnit unit )
            throws InterruptedException
    {
        try
        {
            return tryLock( file, ownerName, label, lockLevel, timeout, unit, ( opLock ) -> true );
        }
        catch ( IOException e )
        {
            Logger logger = LoggerFactory.getLogger( getClass() );
            logger.error( "SHOULD NEVER HAPPEN: IOException trying to lock: " + file, e );
        }

        return false;
    }

    private <T> T tryLock( File f, String ownerName, String label, LockLevel lockLevel, long timeout, TimeUnit unit,
                           LockedFileOperation<T> operation )
            throws InterruptedException, IOException
    {
        return withOpLock( f, ( opLock ) -> {
            long end = timeout < 1 ? -1 : System.currentTimeMillis() + TimeUnit.MILLISECONDS.convert( timeout, unit );

            Logger logger = LoggerFactory.getLogger( getClass() );
            logger.trace( "{}: Trying to lock until: {}", System.currentTimeMillis(), end );

            String name = f.getAbsolutePath();
            FileEntry entry = null;
            try
            {
                while ( end < 1 || System.currentTimeMillis() < end )
                {
                    entry = getLockingEntry( f );
                    if ( entry == null )
                    {
                        if ( read == lockLevel && !f.exists() )
                        {
                            throw new IOException( f + " does not exist. Cannot read-lock missing file!" );
                        }

                        entry = new FileEntry( name, label, lockLevel, ownerName );
                        logger.trace( "No lock; locking as: {} from: {}", lockLevel, label );
                        entryMap.put( name, entry );
                        try
                        {
                            return operation.execute( opLock );
                        }
                        catch ( IOException | RuntimeException e )
                        {
                            // we just locked this, and the call failed...reverse the lock operation.
                            // NOTE: This will CLEAR all locks, which is what we want since there was no FileEntry before.
                            clearLocks( f );
                            throw e;
                        }
                    }
                    else
                    {
                        //                synchronized ( entry )
                        //                {
                        if ( entry.name.equals( f.getAbsolutePath() ) )
                        {
                            if ( entry.lock.lock( ownerName, label, lockLevel ) )
                            {
                                logger.trace( "Added lock to existing entry: {}", entry.name );
                                try
                                {
                                    return operation.execute( opLock );
                                }
                                catch ( IOException | RuntimeException e )
                                {
                                    // we just locked this, and the call failed...reverse the lock operation.
                                    entry.lock.unlock( ownerName );
                                    throw e;
                                }
                            }
                            else
                            {
                                logger.trace( "Lock failed, but retry may allow another attempt..." );
                            }
                        }

                        logger.trace( "Waiting for lock to clear; locking as: {} from: {}", lockLevel, label );
                        //                    entry.wait( 100 );
                        opLock.await( WAIT_TIMEOUT );
                        //                }
                    }
                }
            }
            finally
            {
                // no matter what else happens, do NOT allow a delete lock to remain
                if ( entry != null && entry.lock.getLockLevel() == LockLevel.delete && entry.lock.isLocked() )
                {
                    clearLocks( f );
                }
            }

            logger.trace( "{}: {}: Lock failed", System.currentTimeMillis(), name );
            return null;
        } );
    }

    <T> T setOrJoinFile( File realFile, StreamCallbacks callbacks, boolean doOutput, long timeout,
                                TimeUnit unit, JoinFileOperation<T> function )
            throws IOException, InterruptedException
    {
        long end = timeout < 1 ? -1 : System.currentTimeMillis() + TimeUnit.MILLISECONDS.convert( timeout, unit );

        Logger logger = LoggerFactory.getLogger( getClass() );
        while ( end < 1 || System.currentTimeMillis() < end )
        {
            T result = tryLock( realFile, null, "Open File for " + ( doOutput ? "output" : "input" ),
                                           doOutput ? LockLevel.write : read, WAIT_TIMEOUT, unit, ( opLock ) -> {
                        FileEntry entry = entryMap.get( realFile.getAbsolutePath() );
                        if ( entry.file != null )
                        {
                            if ( doOutput )
                            {
                                throw new IOException( "File already opened for writing: " + realFile );
                            }
                            else if ( !entry.file.isJoinable() )
                            {
                                // If we're joining the file and the file is in the process of closing, we need to wait and
                                // try again once the file has finished closing.

                                logger.trace( "File open but in process of closing; not joinable. Will wait..." );

                                // undo the lock we just placed on this entry, to allow it to clear...
                                entry.lock.unlock( Thread.currentThread().getName() );

                                opLock.signal();

                                logger.trace( "Waiting for file to close at: {}", System.currentTimeMillis() );
                                opLock.await( WAIT_TIMEOUT );

                                logger.trace( "Proceeding with lock attempt at: {}", System.currentTimeMillis() );
                            }
                            else
                            {
                                logger.trace( "Got joinable file" );
                                return function.execute( entry.file );
                            }
                        }
                        else
                        {
                            logger.trace( "No pre-existing open file; opening" );
                            JoinableFile joinableFile = new JoinableFile( realFile, entry.lock,
                                                                          new FileTreeCallbacks( callbacks, entry,
                                                                                                 realFile ), doOutput, opLock );

                            entry.file = joinableFile;
                            return function.execute( joinableFile );
                        }

                        return null;
                    } );

            if ( result != null )
            {
                return result;
            }
        }

        logger.trace( "Failed to lock file for {}", doOutput ? "writing" : "reading" );
        return function.execute( null );
    }

    boolean delete( File file, long timeout, TimeUnit unit )
            throws InterruptedException, IOException
    {
        return tryLock( file, null, "Delete File", LockLevel.delete, timeout, unit, ( opLock ) -> {
            FileEntry entry = entryMap.remove( file.getAbsolutePath() );
            //            synchronized ( this )
            //            {
            opLock.signal();
            //            }

            if ( file.exists() )
            {
                FileUtils.forceDelete( file );
            }

            return true;
        } ) == Boolean.TRUE;
    }

    private FileEntry getLockingEntry( File file )
    {
        Logger logger = LoggerFactory.getLogger( getClass() );
        FileEntry entry = null;

        // search self and ancestors...
        File f = file;
        do
        {
            entry = entryMap.get( f.getAbsolutePath() );
            if ( entry != null )
            {
                logger.trace( "Locked by: {}", entry.lock.getLockInfo() );
                return entry;
            }
            else
            {
                logger.trace( "No lock found for: {}", f );
            }

            f = f.getParentFile();
        }
        while ( f != null );

        // search for children...
        if ( file.isDirectory() )
        {
            String fp = file.getAbsolutePath();
            Optional<String> result =
                    entryMap.keySet().stream().filter( ( path ) -> path.startsWith( fp ) ).findFirst();
            if ( result.isPresent() )
            {
                logger.trace( "Child: {} is locked; returning child as locking entry", result.get() );
                return entryMap.get( result.get() );
            }
        }

        return null;
    }

    private <T> T withOpLock( File f, LockedFileOperation<T> op )
            throws IOException, InterruptedException
    {
        String path = f.getAbsolutePath();
        FileOperationLock opLock;
        synchronized ( operationLocks )
        {
            opLock = operationLocks.get( path );
            if ( opLock == null )
            {
                opLock = new FileOperationLock();
                operationLocks.put( path, opLock );
            }
        }

        return opLock.lockAnd( op );
    }

    private static final class FileEntry
    {
        private final String name;

        private final LockOwner lock;

        private JoinableFile file;

        FileEntry( String name, String lockingLabel, LockLevel lockLevel )
        {
            this.name = name;
            this.lock = new LockOwner( Thread.currentThread().getName(), lockingLabel, lockLevel );
        }

        FileEntry( String name, String lockingLabel, LockLevel lockLevel, String ownerName )
        {
            this.name = name;
            this.lock = new LockOwner( ownerName, lockingLabel, lockLevel );
        }
    }

    private final class FileTreeCallbacks
            implements StreamCallbacks
    {
        private StreamCallbacks callbacks;

        private File file;

        private FileEntry entry;

        public FileTreeCallbacks( StreamCallbacks callbacks, FileEntry entry, File file )
        {
            this.callbacks = callbacks;
            this.file = file;
            this.entry = entry;
        }

        @Override
        public void flushed()
        {
            if ( callbacks != null )
            {
                callbacks.flushed();
            }
        }

        @Override
        public void beforeClose()
        {
            if ( callbacks != null )
            {
                callbacks.beforeClose();
            }

            //            synchronized( entry )
            //            {
            //                //            FileTree.this.lock.lock();
            //                entry.file = null;
            //                //            FileTree.this.lock.unlock();
            //            }
        }

        @Override
        public void closed()
        {
            if ( callbacks != null )
            {
                callbacks.closed();
            }

            Logger logger = LoggerFactory.getLogger( getClass() );
            logger.trace( "unlocking: {}", file );

            try
            {
                withOpLock( file, ( opLock ) -> {
                    entry.file = null;
                    clearLocks( file );
                    return null;
                } );
            }
            catch ( IOException e )
            {
                logger.error( "Failed to mark as closed: " + e.getMessage(), e );
            }
            catch ( InterruptedException e )
            {
                logger.error( "Interrupted while marking as closed." );
            }
            //            //            synchronized ( entry )
            //            synchronized ( FileTree.this )
            //            {
            //                entry.file = null;
            //                FileTree.this.clearLocks( file );
            //            }
        }
    }

     @FunctionalInterface
    interface JoinFileOperation<T>
    {
        T execute( JoinableFile file )
                throws IOException;
    }
}