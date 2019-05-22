/**
 * Copyright (C) 2015 Red Hat, Inc. (nos-devel@redhat.com)
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
package org.commonjava.util.partyline.util;

import org.commonjava.util.partyline.FileTree;
import org.commonjava.util.partyline.spi.JoinableFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class FileTreeUtils
{
    private static final Logger logger = LoggerFactory.getLogger( FileTreeUtils.class );

    /**
     * When trying to lock a file, we first must ensure that no directory further up the hierarchy is already locked with
     * a more restrictive lock. If we're trying to lock a directory, we also must ensure that no child directory/file
     * is locked with a more restrictive lock. This method checks for those cases, and returns the {@link FileTree.FileEntry}
     * from the ancestry or descendent files/directories that is already locked.
     *
     * This should prevent us from deleting a directory when a child file within that directory structure is being read
     * or written. Likewise, it should prevent us from reading or writing a file in a directory already locked for
     * deletion.
     *
     * @param file The file whose context directories / files should be checked for locks
     * @return The nearest {@link FileTree.FileEntry}, corresponding to a locked file. Parent directories returned before children.
     */
    public static FileTree.FileEntry getNearestLockingEntry( Map<String, FileTree.FileEntry> entryMap, File file )
    {
        FileTree.FileEntry entry;

        // search self and ancestors...
        File f = file;
        do
        {
            entry = entryMap.get( f.getAbsolutePath() );
            if ( entry != null )
            {
                logger.trace( "Locked by: {}", entry.getLockOwner().getLockInfo() );
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
            final String fp = file.getAbsolutePath() + File.separator;
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

    /**
     * Iterate all {@link FileTree.FileEntry instances} to extract information about active locks.
     *
     * @param fileConsumer The operation to extract information from a single active file.
     */
    public static void forAll( Map<String, FileTree.FileEntry> entryMap, Consumer<JoinableFile> fileConsumer )
    {
        forAll( entryMap, entry -> entry.getFile() != null, entry -> fileConsumer.accept( entry.getFile() ) );
    }

    /**
     * Iterate all {@link FileTree.FileEntry instances} to extract information about active locks.
     *
     * @param predicate The selector determining which files to analyze.
     * @param fileConsumer The operation to extract information from a single active file.
     */
    public static void forAll( Map<String, FileTree.FileEntry> entryMap,
                               Predicate<? super FileTree.FileEntry> predicate,
                               Consumer<FileTree.FileEntry> fileConsumer )
    {
        TreeMap<String, FileTree.FileEntry> sorted = new TreeMap<>( entryMap );
        sorted.forEach( ( key, entry ) -> {
            if ( entry != null && predicate.test( entry ) )
            {
                fileConsumer.accept( entry );
            }
        } );
    }

    /**
     * Render the active files as a tree structure, for output to a log file or other string-oriented output.
     */
    public static String renderTree( Map<String, FileTree.FileEntry> entryMap )
    {
        TreeMap<String, FileTree.FileEntry> sorted = new TreeMap<>( entryMap );
        StringBuilder sb = new StringBuilder();
        sorted.forEach( ( key, entry ) -> {
            sb.append( "+- " );
            Stream.of( key.split( "/" ) ).forEach( ( part ) -> sb.append( "  " ) );

            sb.append( new File( key ).getName() );
            if ( entry.getFile() != null )
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
}
