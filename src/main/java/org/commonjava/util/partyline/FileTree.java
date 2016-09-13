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

import org.apache.commons.io.IOUtils;
import org.commonjava.util.partyline.callback.StreamCallbacks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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

/**
 * Created by jdcasey on 8/18/16.
 */
public class FileTree
{

    private final String name;

    private final FileTree parent;

    private LockOwner lock;

    private JoinableFile file;

    private Map<String, FileTree> subTrees = new ConcurrentHashMap<>();

    public FileTree()
    {
        this.parent = null;
        this.name = "/";
        this.file = null;
    }

    private FileTree( FileTree parent, String name )
    {
        this.parent = parent;
        this.name = name;
        this.file = null;
    }

    @Override
    public String toString()
    {
        return "FileTree@" + super.hashCode() + "{" +
                "name='" + name + '\'' +
                '}';
    }

    public void forAll( Consumer<JoinableFile> fileConsumer )
    {
        searchAnd( this, "FOR_ALL", ( tree ) -> true, ( tree ) -> {
            if ( tree.file != null )
            {
                fileConsumer.accept( tree.file );
            }
        } );
    }

    public void forFilesOwnedBy( long ownerId, Consumer<JoinableFile> fileConsumer )
    {
        searchAnd( this, "ALL_OWNED_BY", ( tree ) -> ( tree.subTrees != null && !tree.subTrees.isEmpty() ), ( tree ) -> {
            if ( tree.file != null && tree.file.isOwnedBy( ownerId ) )
            {
                fileConsumer.accept( tree.file );
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
        if ( subTrees != null )
        {
            if ( indent > 0 )
            {
                sb.append( "|- " );
            }

            sb.append( name );

            if ( file != null )
            {
                sb.append( " (F)" );
            }

            for ( Iterator<String> it = subTrees.keySet().iterator(); it.hasNext(); )
            {
                String name = it.next();
                FileTree tree = subTrees.get( name );

                sb.append( '\n' );
                for ( int i = 0; i < indent; i++ )
                {
                    sb.append( "|  " );
                }
                tree.renderTreeInternal( sb, indent + 1 );
            }
        }
        else
        {
            sb.append( "+ " ).append( name ).append( file == null ? "" : " (F)" );
        }
    }

    public <T> T findNodeAnd( File file, Function<FileTree, T> function, T defValue )
    {
        return traversePathAnd( file.getPath(), "FIND IN TREE", ( ref, part ) -> {
            FileTree current = ref.get();
            if ( current != null )
            {
                ref.set( current.subTrees.get( part ) );
            }

            return ref.get() != null;
        }, (node)->function.apply( node ), defValue );
    }

    public <T> T withNode( File file, boolean autoCreate, long timeoutMs, Function<FileTree, T> function, T defValue )
    {
        Logger logger = LoggerFactory.getLogger( getClass() );

        return traversePathAnd( file.getPath(), "MODIFY TREE", ( ref, part ) -> {
            FileTree current = ref.get();
            if ( current != null )
            {
                if ( autoCreate )
                {
                    boolean result = lockAnd(current, timeoutMs, true, ()->{
                        FileTree child = null;
                        child = current.subTrees.get( part );
                        logger.trace( "{} child of: {} is: {}", part, current, child );
                        if ( child == null )
                        {
                            child = new FileTree( current, part );
                            current.subTrees.put( part, child );
                            logger.trace( "Created child: {} of: {}", child, current );
                        }
                        ref.set( child );
                        return true;
                    }, false );

                    if ( !result )
                    {
                        ref.set( null );
                    }
                }
                else
                {
                    ref.set( current.subTrees.get( part ) );
                }
            }

            return ref.get() != null;
        }, (node)->lockAnd(node, timeoutMs, false, ()->function.apply( node ), defValue ), defValue );
    }

    private <T> T lockAnd( FileTree current, long timeoutMs, boolean unlock, Supplier<T> resultSupplier, T defaultValue )
    {
        if ( current == null )
        {
            return defaultValue;
        }
        Logger logger = LoggerFactory.getLogger( getClass() );
        boolean locked = false;
        try
        {
            logger.trace( "LOCK {}", current.name );
            if ( timeoutMs > 1000 )
            {
                locked = current.tryLock( timeoutMs, TimeUnit.MILLISECONDS );
            }
            else
            {
                locked = current.tryLock( 1000, TimeUnit.MILLISECONDS );
            }

            if ( locked )
            {
                logger.trace( "locked" );
            }
            else
            {
                logger.trace( "LOCK FAILED: {}", current.name );
            }

            if ( locked && resultSupplier != null )
            {
                return resultSupplier.get();
            }
        }
        catch ( InterruptedException e )
        {
            logger.warn( "Interrupted while trying to withNode FileTree at: {}. Aborting...", file );
        }
        finally
        {
            if ( unlock && locked )
            {
                logger.trace( "UNLOCK: {}", current.name );
                current.unlock();
                logger.trace( "unlocked" );
            }
        }

        return defaultValue;
    }

    public boolean isLocked()
    {
        return lock != null;
    }

    public synchronized void unlock()
    {
        if ( file != null )
        {
            IOUtils.closeQuietly( file );
        }

        if ( lock != null )
        {
            lock = null;
            notifyAll();
        }
    }

    private boolean tryLock( long timeoutMs, TimeUnit milliseconds )
            throws InterruptedException
    {
        long end = timeoutMs > 0 ? System.currentTimeMillis() + timeoutMs : -1;

        Logger logger = LoggerFactory.getLogger( getClass() );
        logger.trace( "{}: Trying to lock until: {}", System.currentTimeMillis(), end );
        while ( end < 0 || System.currentTimeMillis() < end )
        {
            synchronized ( this )
            {
                wait( 100 );
                if ( lock == null )
                {
                    lock = new LockOwner();
                    notifyAll();
                    return true;
                }
                else if ( lock.getThreadId() == Thread.currentThread().getId() )
                {
                    return true;
                }
            }
        }

        logger.trace( "{}: Lock failed", System.currentTimeMillis() );
        return false;
    }

    private synchronized <T> T traversePathAnd( String path, String label,
                                                BiFunction<AtomicReference<FileTree>, String, Boolean> inbound,
                                                Function<FileTree, T> resultProcessor, T defValue )
    {
        if ( isEmpty( path ) )
        {
            if ( subTrees != null && !subTrees.isEmpty() )
            {
                return resultProcessor.apply( this );
            }
            else
            {
                return defValue;
            }
        }

        Logger logger = LoggerFactory.getLogger( getClass() );

        List<FileTree> nodes = new ArrayList<>();
        // TODO: doesn't work on Windows...
        AtomicReference<FileTree> ref = new AtomicReference<>( this );
        logger.trace( "{}: Start at: {}", label, this );

        String[] parts = path.split( "/" );
        if ( parts.length < 1 || ( parts.length == 1 && isEmpty( parts[0] ) ) )
        {
            return defValue;
        }

        Stream.of( parts ).skip( isEmpty( parts[0] ) ? 1 : 0 ).map( ( part ) -> {
            Boolean proceed = inbound.apply( ref, part );
            if ( proceed )
            {
                nodes.add( ref.get() );
            }
            return proceed;
        } ).filter( ( r ) -> !r ).findFirst();

        FileTree current = ref.get();
        logger.trace( "{}: Returning traversal result of: {}", label, current );

        T result;
        if ( current != null && current != this )
        {
            result = resultProcessor.apply( current );
        }
        else
        {
            result = defValue;
        }

        return result;
    }

    private synchronized void searchAnd( FileTree start, String label, Predicate<FileTree> filter, Consumer<FileTree> processor )
    {
        if ( subTrees == null || subTrees.isEmpty() )
        {
            return;
        }

        Logger logger = LoggerFactory.getLogger( getClass() );

        // TODO: doesn't work on Windows...
        List<FileTree> toExamine = new ArrayList<>();
        toExamine.add( start );

        do
        {
            FileTree next;
            synchronized ( toExamine )
            {
                next = toExamine.remove( 0 );
            }

            if ( next.subTrees != null && !next.subTrees.isEmpty() )
            {
                logger.trace( "{}: Examining sub-trees of: {}", label, next );
                next.subTrees.values().parallelStream().filter( filter ).forEach( ( tree ) -> {
                    logger.trace( "{}: processing {}", label, tree );
                    processor.accept( tree );
                    synchronized ( toExamine )
                    {
                        logger.trace( "{}: Adding {} to list of tree awaiting examination.", label, tree );
                        toExamine.add( tree );
                    }
                } );
            }
        }
        while ( !toExamine.isEmpty() );
    }

    public JoinableFile getFile()
    {
        return file;
    }

    public JoinableFile setFile( File realFile, StreamCallbacks callbacks, boolean doOutput )
            throws IOException
    {
        this.file = new JoinableFile( realFile, lock, new FileTreeCallbacks( callbacks ), doOutput );
        return this.file;
    }

    private class FileTreeCallbacks
            implements StreamCallbacks
    {
        private StreamCallbacks callbacks;

        public FileTreeCallbacks( StreamCallbacks callbacks )
        {
            this.callbacks = callbacks;
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

            //            FileTree.this.lock.lock();
            FileTree.this.file = null;
            //            FileTree.this.lock.unlock();
        }

        @Override
        public void closed()
        {
            if ( callbacks != null )
            {
                callbacks.closed();
            }

            Logger logger = LoggerFactory.getLogger( getClass() );
            while ( FileTree.this.isLocked() )
            {
                logger.trace( "unlocking" );
                FileTree.this.unlock();
            }
        }
    }
}