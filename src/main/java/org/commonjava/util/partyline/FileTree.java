package org.commonjava.util.partyline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.lang.Boolean.FALSE;
import static org.apache.commons.lang.StringUtils.isEmpty;

/**
 * Created by jdcasey on 8/18/16.
 */
public class FileTree
{
    private final String name;

    private JoinableFile file;

    private Map<String, FileTree> subTrees = new ConcurrentHashMap<>();

    public FileTree()
    {
        this.name = "/";
        this.file = null;
    }

    private FileTree( String name )
    {
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

    public synchronized JoinableFile remove( String path )
    {
        Logger logger = LoggerFactory.getLogger( getClass() );
        FileTree root = this;
        Map<FileTree, String> emptySegments = new HashMap<>();
        JoinableFile result = traversePathAnd( path, "REMOVE", ( ref, part ) -> {
            FileTree current = ref.get();
            if ( current != null )
            {
                FileTree child = current.subTrees.get( part );
                if ( child != null && child.file == null )
                {
                    logger.trace(
                            "REMOVE: Adding {}/{} to empty segments list for purge in case we find the file we want to remove.",
                            current, part );
                    emptySegments.put( current, part );
                }

                logger.trace( "REMOVE: Continuing path traversal with: {}", child );
                ref.set( child );
            }
        }, ( current ) -> {
            logger.trace( "REMOVE: Clearing {} from: {}", current.file, current );
            JoinableFile file = current.file;
            current.file = null;
            return file;
        }, null );

        // prune back the intermediate empty entries.
        if ( result != null && !emptySegments.isEmpty() )
        {
            emptySegments.forEach( ( parent, childName ) -> {
                logger.trace( "REMOVE: Purging empty intermediate tree segment: {}/{}", parent, childName );
                parent.subTrees.remove( childName );
            } );
        }

        notifyAll();

        return result;
    }

    public synchronized void add( JoinableFile joinableFile )
    {
        Logger logger = LoggerFactory.getLogger( getClass() );
        traversePathAnd( joinableFile.getPath(), "ADD", ( ref, part ) -> {
            FileTree current = ref.get();
            FileTree child = current.subTrees.get( part );
            if ( child == null )
            {
                child = new FileTree( part );
                current.subTrees.put( part, child );
                logger.trace( "Created child: {} of: {}", child, current );
            }
            ref.set( child );
        }, ( current ) -> {
            current.file = joinableFile;
            logger.trace( "Set file: {} on: {}", current.file, current );
            return null;
        }, null );

        notifyAll();
    }

    public boolean hasChildren( File file )
    {
        Logger logger = LoggerFactory.getLogger( getClass() );
        return traversePathAnd( file.getPath(), "HAS_CHILDREN", ( ref, part ) -> {
            FileTree current = ref.get();
            logger.trace( "HAS_CHILREN: At: {}, retrieve: {}", current, part );
            if ( current != null )
            {
                ref.set( current.subTrees == null ? null : current.subTrees.get( part ) );
            }
        }, ( current ) -> current.subTrees != null && !current.subTrees.isEmpty(), FALSE );
    }

    public JoinableFile getFile( File file )
    {
        Logger logger = LoggerFactory.getLogger( getClass() );
        return traversePathAnd( file.getPath(), "GET", ( ref, part ) -> {
            FileTree current = ref.get();
            logger.trace( "GET: At: {}, retrieve: {}", current, part );

            if ( current != null )
            {
                ref.set( current.subTrees == null ? null : current.subTrees.get( part ) );
            }
        }, ( current ) -> current.file, null );
    }

    public JoinableFile findAncestorFile( File file, Predicate<JoinableFile> filter )
    {
        List<FileTree> reversedPath = new ArrayList<>();

        traversePathAnd( file.getPath(), "FIND_ANCESTOR", ( ref, part ) -> {
            FileTree current = ref.get();
            FileTree child = current.subTrees == null ? null : current.subTrees.get( part );
            if ( child != null )
            {
                reversedPath.add( 0, child );
                ref.set( child );
            }
        }, ( current ) -> null, null );

        Optional<JoinableFile> result = reversedPath.stream()
                                                    .filter( ( tree ) -> tree.file != null && filter.test( tree.file ) )
                                                    .map( ( tree ) -> tree.file )
                                                    .findFirst();
        return result.isPresent() ? result.get() : null;
    }

    public JoinableFile findChildFile( File file, Predicate<JoinableFile> filter )
    {
        Logger logger = LoggerFactory.getLogger( getClass() );
        FileTree tree = traversePathAnd( file.getPath(), "FIND_DESCENDANT", ( ref, part ) -> {
            FileTree current = ref.get();
            if ( current != null )
            {
                ref.set( current.subTrees.get( part ) );
            }
        }, ( current ) -> current, null );

        if ( tree == null )
        {
            return null;
        }

        AtomicReference<JoinableFile> result = new AtomicReference<>();
        searchAnd(tree, "FIND_DESCENDANT", (t)->true, (t)->{
            if ( result.get() == null && t.file != null && filter.test( t.file ) )
            {
                logger.trace( "FIND_DESCENDANT: Selecting file from sub-tree node: {}", tree );
                result.set( tree.file );
            }
        });

        return result.get();
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

    private synchronized <T> T traversePathAnd( String path, String label,
                                                BiConsumer<AtomicReference<FileTree>, String> traverser,
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

        // TODO: doesn't work on Windows...
        AtomicReference<FileTree> ref = new AtomicReference<>( this );
        logger.trace( "{}: Start at: {}", label, this );

        String[] parts = path.split( "/" );
        if ( parts.length < 1 || ( parts.length == 1 && isEmpty( parts[0] ) ) )
        {
            return defValue;
        }

        Stream.of( parts).skip( isEmpty( parts[0])?1:0).forEach( ( part ) -> traverser.accept( ref, part ) );

        FileTree current = ref.get();
        logger.trace( "{}: Returning traversal result of: {}", label, current );
        if ( current != null && current != this )
        {
            return resultProcessor.apply( current );
        }
        else
        {
            return defValue;
        }
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

}