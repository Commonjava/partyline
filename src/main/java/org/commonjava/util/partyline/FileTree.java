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
            if ( entry.lock.unlock() )
            {
                entryMap.remove( entry.name );
                notifyAll();
                return true;
            }
            else
            {
                logger.trace( "Unlock request failed: Remaining locks:\n\n{}", entry.lock.getLockInfo() );
            }
        }

        return false;
    }

    public boolean tryLock( File f, String label, LockLevel lockLevel, long timeout, TimeUnit unit )
            throws InterruptedException
    {
        long end = timeout > 0 ? System.currentTimeMillis() + TimeUnit.MILLISECONDS.convert(timeout, unit) : -1;

        Logger logger = LoggerFactory.getLogger( getClass() );
        logger.trace( "{}: Trying to lock until: {}", System.currentTimeMillis(), end );

        String name = f.getAbsolutePath();
        while ( end < 0 || System.currentTimeMillis() < end )
        {
            synchronized ( this )
            {
                FileEntry entry = getLockingEntry( f );
                if ( entry != null && entry.name.equals( f.getAbsolutePath() ) && entry.lock.lock( label, lockLevel ) )
                {
                    logger.trace( "Added lock to existing entry: {}", entry.name );
                    return true;
                }
                else if ( entry == null || entry.lock.getLockLevel() == read )
                {
                    logger.trace( "Lock cleared; locking as: {} from: {}", lockLevel, label );
                    entryMap.put( name, new FileEntry( name, label, lockLevel ) );
                    return true;
                }

                logger.trace( "Waiting for lock to clear; locking as: {} from: {}", lockLevel, label );
                wait( 100 );
            }
        }

        logger.trace( "{}: {}: Lock failed", System.currentTimeMillis(), name );
        return false;
    }

    public JoinableFile setOrJoinFile( File realFile, StreamCallbacks callbacks, boolean doOutput, long timeout, TimeUnit unit )
            throws IOException, InterruptedException
    {
        boolean removed = false;
        JoinableFile result = null;

        if ( tryLock( realFile, "Open File for " + ( doOutput ? "output" : "input" ),
                      doOutput ? LockLevel.write : read, timeout, unit ) )
        {
            FileEntry entry = entryMap.get( realFile.getAbsolutePath() );

            synchronized ( entry )
            {
                if ( entry.file != null )
                {
                    if ( doOutput && entry.file.isWriteLocked() )
                    {
                        if ( entry.lock.unlock() )
                        {
                            entryMap.remove( realFile.getAbsolutePath() );
                            removed = true;
                        }

                        throw new IOException( "POSSIBLE CORRUPTION DETECTED! OutputStream was already open for unlocked file: " + realFile );
                    }

                    result = entry.file;
                }
                else
                {
                    JoinableFile joinableFile =
                            new JoinableFile( realFile, entry.lock, new FileTreeCallbacks( callbacks, entry, realFile ), doOutput );

                    entry.file = joinableFile;
                    result = joinableFile;
                }
            }
        }

        if ( removed )
        {
            synchronized ( this )
            {
                notifyAll();
            }
        }

        return result;
    }

    public boolean delete( File file, long timeout, TimeUnit unit )
            throws InterruptedException, IOException
    {
        if ( tryLock( file, "Delete File", LockLevel.delete, timeout, unit ) )
        {
            FileEntry entry = entryMap.remove( file.getAbsolutePath() );
            synchronized ( this )
            {
                notifyAll();
            }

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

            synchronized( entry )
            {
                //            FileTree.this.lock.lock();
                entry.file = null;
                //            FileTree.this.lock.unlock();
            }
        }

        @Override
        public void closed()
        {
            if ( callbacks != null )
            {
                callbacks.closed();
            }

            Logger logger = LoggerFactory.getLogger( getClass() );
            logger.trace( "unlocking" );
            FileTree.this.unlock( file );
        }
    }
}