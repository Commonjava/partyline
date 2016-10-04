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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.apache.commons.lang.StringUtils.isEmpty;
import static org.apache.commons.lang.StringUtils.join;
import static org.commonjava.util.partyline.LockLevel.read;

/**
 * Created by jdcasey on 8/18/16.
 */
public class FileTree
{

    private static final long DEFAULT_LOCK_TIMEOUT = 2000;

    private static final long WAIT_TIMEOUT = 100;

    private ConcurrentHashMap<String, FileEntry> entryMap = new ConcurrentHashMap<String, FileEntry>();

    private static final class FileEntry
    {
        private final String name;
        private final LockOwner lock;
        private JoinableFile file;

        FileEntry( String name, String lockingLabel, LockLevel lockLevel )
        {
            this.name = name;
            this.lock = new LockOwner( lockingLabel, lockLevel );
        }
    }

    public void forAll( String operationName, Consumer<JoinableFile> fileConsumer )
    {
        TreeMap<String, FileEntry> sorted = new TreeMap<>( entryMap );
        sorted.forEach( ( key, entry ) -> {
            if ( entry.file != null )
            {
                fileConsumer.accept( entry.file );
            }
        } );
    }

    public void forFilesOwnedBy( long ownerId, String operationName, Consumer<JoinableFile> fileConsumer )
    {
        TreeMap<String, FileEntry> sorted = new TreeMap<>( entryMap );
        sorted.forEach( ( key, entry ) -> {
            if ( entry.file != null && entry.lock.getThreadId() == ownerId )
            {
                fileConsumer.accept( entry.file );
            }
        } );
    }

    public String renderTree()
    {
        StringBuilder sb = new StringBuilder();
        renderTreeInternal( sb, 0 );
        return sb.toString();
    }

    private synchronized void renderTreeInternal( StringBuilder sb, int indent )
    {
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
        } while( f != null );

        // search for children...
        String fp = file.getAbsolutePath();
        Optional<String> result = entryMap.keySet().stream().filter( ( path ) -> path.startsWith( fp ) ).findFirst();
        if ( result.isPresent() )
        {
            logger.trace( "Child: {} is locked; returning child as locking entry", result.get() );
            return entryMap.get( result.get() );
        }

        return null;
    }

    public LockLevel getLockLevel( File file )
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

    public synchronized boolean unlock( File f )
    {
        Logger logger = LoggerFactory.getLogger( getClass() );
        FileEntry entry = entryMap.get( f.getAbsolutePath() );
        if ( entry != null )
        {
//            synchronized ( entry )
//            {
                logger.trace( "Unlocking {}", f );
                if ( entry.lock.unlock() )
                {
                    logger.trace( "Unlocked; clearing resources associated with lock" );
                    if ( entry.file != null )
                    {
                        logger.trace( "Closing file..." );
                        IOUtils.closeQuietly( entry.file );
                        entry.file = null;
                    }

                    entryMap.remove( entry.name );
//                    entry.notifyAll();
                    notifyAll();
                    logger.trace( "Unlock succeeded." );
                    return true;
                }
                else
                {
                    logger.trace( "Unlock request failed: Remaining locks:\n\n{}", entry.lock.getLockInfo() );
                }
//            }
        }
        else
        {
            logger.trace( "{} not locked", f );
        }

        logger.trace( "Unlock failed." );
        return false;
    }

    public synchronized boolean tryLock( File f, String label, LockLevel lockLevel, long timeout, TimeUnit unit )
            throws InterruptedException
    {
        long end = System.currentTimeMillis() + timeout > 0 ? TimeUnit.MILLISECONDS.convert(timeout, unit) : DEFAULT_LOCK_TIMEOUT;

        Logger logger = LoggerFactory.getLogger( getClass() );
        logger.trace( "{}: Trying to lock until: {}", System.currentTimeMillis(), end );

        String name = f.getAbsolutePath();
        while ( System.currentTimeMillis() < end )
        {
            FileEntry entry = getLockingEntry( f );
            if ( entry == null )
            {
                logger.trace( "No lock; locking as: {} from: {}", lockLevel, label );
                entryMap.put( name, new FileEntry( name, label, lockLevel ) );
                return true;
            }
            else if ( entry != null )
            {
//                synchronized ( entry )
//                {
                    if ( entry.name.equals( f.getAbsolutePath() ) && entry.lock.lock( label, lockLevel ) )
                    {
                        logger.trace( "Added lock to existing entry: {}", entry.name );
                        return true;
                    }

                    logger.trace( "Waiting for lock to clear; locking as: {} from: {}", lockLevel, label );
//                    entry.wait( 100 );
                    wait( WAIT_TIMEOUT );
//                }
            }
        }

        logger.trace( "{}: {}: Lock failed", System.currentTimeMillis(), name );
        return false;
    }

    public synchronized JoinableFile setOrJoinFile( File realFile, StreamCallbacks callbacks, boolean doOutput, long timeout, TimeUnit unit )
            throws IOException, InterruptedException
    {
        long end = System.currentTimeMillis() + timeout > 0 ? TimeUnit.MILLISECONDS.convert(timeout, unit) : DEFAULT_LOCK_TIMEOUT;

        Logger logger = LoggerFactory.getLogger( getClass() );
        while ( System.currentTimeMillis() < end )
        {
            if ( tryLock( realFile, "Open File for " + ( doOutput ? "output" : "input" ),
                          doOutput ? LockLevel.write : read, WAIT_TIMEOUT, unit ) )
            {
                FileEntry entry = entryMap.get( realFile.getAbsolutePath() );

//                synchronized ( entry )
//                {
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
                            entry.lock.unlock();
//                            entry.notifyAll();
                            notifyAll();
                            logger.trace( "Waiting for file to close at: {}", System.currentTimeMillis() );
//                            entry.wait( 100 );
                            wait( WAIT_TIMEOUT );
                            logger.trace( "Proceeding with lock attempt at: {}", System.currentTimeMillis() );
                        }
                        else
                        {
                            logger.trace( "Got joinable file" );
                            return entry.file;
                        }
                    }
                    else
                    {
                        logger.trace( "No pre-existing open file; opening" );
                        JoinableFile joinableFile =
                                new JoinableFile( realFile, entry.lock, new FileTreeCallbacks( callbacks, entry, realFile ), doOutput );

                        entry.file = joinableFile;
                        return joinableFile;
                    }
//                }
            }
        }

        logger.trace( "Failed to lock file for {}", doOutput ? "writing" : "reading" );
        return null;
    }

    public synchronized boolean delete( File file, long timeout, TimeUnit unit )
            throws InterruptedException, IOException
    {
        if ( tryLock( file, "Delete File", LockLevel.delete, timeout, unit ) )
        {
            FileEntry entry = entryMap.remove( file.getAbsolutePath() );
//            synchronized ( this )
//            {
                notifyAll();
//            }

            FileUtils.forceDelete( file );
            return true;
        }

        return false;
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
//            synchronized ( entry )
            synchronized ( FileTree.this )
            {
                entry.file = null;
                FileTree.this.unlock( file );
            }
        }
    }
}