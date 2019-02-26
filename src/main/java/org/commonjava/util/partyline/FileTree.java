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
package org.commonjava.util.partyline;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.commonjava.util.partyline.callback.StreamCallbacks;
import org.commonjava.util.partyline.lock.LockLevel;
import org.commonjava.util.partyline.lock.UnlockStatus;
import org.commonjava.util.partyline.lock.global.GlobalLockManager;
import org.commonjava.util.partyline.lock.local.LocalLockManager;
import org.commonjava.util.partyline.lock.local.ReentrantOperation;
import org.commonjava.util.partyline.lock.local.LocalLockOwner;
import org.commonjava.util.partyline.spi.JoinableFile;
import org.commonjava.util.partyline.spi.JoinableFilesystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.commonjava.util.partyline.lock.LockLevel.read;
import static org.commonjava.util.partyline.lock.local.LocalLockOwner.getLockReservationName;
import static org.commonjava.util.partyline.util.FileTreeUtils.getNearestLockingEntry;

/**
 * Maintains access to files in partyline. This class restricts operations to prohibit concurrent operations on the same
 * path at the same time, where 'operation' means opening a file, deleting a file, or locking/unlocking a file. It also
 * provides methods for extracting or logging information about the file locks that are currently active.
 */
public final class FileTree
{

    public static final long DEFAULT_LOCK_TIMEOUT = 5000;

    private static final long WAIT_TIMEOUT = 100;

    private final Logger logger = LoggerFactory.getLogger( getClass() );

    private final Map<String, FileEntry> entryMap = new ConcurrentHashMap<>();

    private final JoinableFilesystem filesystem;

    private final LocalLockManager lockManager;

    private final GlobalLockManager globalLockManager;

    FileTree( JoinableFilesystem filesystem )
    {
        this( filesystem, null );
    }

    FileTree( JoinableFilesystem filesystem, GlobalLockManager globalLockManager ) // for cluster env
    {
        this.filesystem = filesystem;
        this.lockManager = filesystem.getLocalLockManager();
        this.globalLockManager = globalLockManager;
    }

    public Map<String, FileTree.FileEntry> getUnmodifiableEntryMap()
    {
        return Collections.unmodifiableMap( entryMap );
    }

    /**
     * Retrieve the {@link LockLevel} for the given file. This corresponds to the highest level of access currently
     * granted for this file.
     *
     * @see LockLevel
     */
    LockLevel getLockLevel( File file )
    {
        FileEntry entry = getNearestLockingEntry( getUnmodifiableEntryMap(), file );
        logger.trace( "Locking entry for file: {} is: {}", file, entry );
        String path = file.getAbsolutePath();
        if ( entry == null )
        {
            return null;
        }
        else if ( !entry.name.equals( path ) )
        {
            logger.trace( "Returning parent lock level lock due to parent lock (level: {})",
                          entry.lockOwner.getLockLevel() );
            return entry.lockOwner.getLockLevel();
        }
        else
        {
            logger.trace( "Returning lock level for this file as: {}", entry.lockOwner.getLockLevel() );
            return entry.lockOwner.getLockLevel();
        }
    }

    int getContextLockCount( File file )
    {
        FileEntry entry = getNearestLockingEntry( getUnmodifiableEntryMap(), file );
        if ( entry == null )
        {
            return 0;
        }
        else if ( !entry.name.equals( file.getAbsolutePath() ) )
        {
            //FIXME: Not sure if this is also 0
            return 0;
        }
        else
        {
            return entry.lockOwner.getContextLockCount();
        }
    }

    /**
     * (Manually) unlock a file for a given ownership label. The label allows the system to avoid unlocking for other
     * active threads that might still be using the file.
     * @param f The file to unlock
     * @param label The label for the lock to remove
     * @return true if the file has no remaining locks after unlocking for this owner; false otherwise
     */
    boolean unlock( File f, final String label )
                    throws IOException
    {
        try
        {
            return lockManager.reentrantSynchronous( f.getAbsolutePath(), ( opLock ) -> {
                String ownerName = getLockReservationName();
                FileEntry entry = entryMap.get( f.getAbsolutePath() );
                if ( entry != null )
                {
                    logger.trace( "Unlocking {} (owner: {})", f, ownerName );
                    UnlockStatus unlockStatus = entry.lockOwner.unlock( label );
                    filesystem.updateDominantLocks( f.getAbsolutePath(), unlockStatus );

                    if ( unlockStatus.isUnlocked() )
                    {
                        logger.trace( "Unlocked; clearing resources associated with lock" );

                        closeEntryFile( entry, ownerName );

                        if ( !unlockAssociatedEntries( entry, label ) )
                        {
                            return false;
                        }

                        if ( !entry.lockOwner.isLocked() )
                        {
                            removeEntryAndReentrantLock( entry.name );
                        }

                        opLock.signal();
                        logger.trace( "Unlock succeeded." );
                        return true;
                    }
                    else
                    {
                        logger.trace( "{} Request did not completely unlock file. Remaining locks:\n\n{}", ownerName,
                                      entry.lockOwner.getLockInfo() );
                        opLock.signal();
                        return false;
                    }
                }
                else
                {
                    logger.trace( "{} not locked by {}", f, ownerName );
                }

                opLock.signal();
                return true;
            } );
        }
        catch ( IOException e )
        {
            logger.error( "SHOULD NEVER HAPPEN: IOException trying to unlock: " + f, e );
        }
        catch ( InterruptedException e )
        {
            logger.warn( "Interrupted while trying to unlock: " + f );
        }

        return false;
    }

    private void removeEntryAndReentrantLock( String path )
    {
        entryMap.remove( path );
        lockManager.removeReentrantLock( path );
    }

    private boolean unlockAssociatedEntries( final FileEntry entry, final String label )
                    throws IOException
    {
        // the 'alsoLocked' entry field constitutes a linked list of locked entries.
        // When we unlock the topmost one, we need to unlock the ones that are linked too.
        FileEntry alsoLocked = entry.alsoLocked;
        while ( alsoLocked != null )
        {
            logger.trace( "ALSO Unlocking: {}", alsoLocked.name );
            UnlockStatus unlockStatus = alsoLocked.lockOwner.unlock( label );

            String fname = alsoLocked.file != null ? alsoLocked.file.getPath() : alsoLocked.name;
            filesystem.updateDominantLocks( fname, unlockStatus );

            //
            //            {
            //                // FIXME: This is probably a little bit wrong, but in practice it should never fail.
            //                // I'm not sure how we should handle failure to decrement the lock count for this
            //                // ThreadContext. Should it cause the main unlock() method here to fail? Probably...
            //                logger.error( "FAILED to unlock associated entry for path: {}\n\nEntry: {}\n\n", alsoLocked, entry.name );
            //
            //                opLock.signal();
            //                logger.trace( "Unlock failed for: {}", entry.name );
            //                return false;
            //            }

            if ( !alsoLocked.lockOwner.isLocked() )
            {
                removeEntryAndReentrantLock( alsoLocked.name );
            }

            alsoLocked = alsoLocked.alsoLocked;
        }

        return true;
    }

    /**
     * In certain cases, when an operation completes we cannot retain any locks on the file. This method clears all
     * remaining locks and releases the file from the active-locked mapping. The cases where this is important:
     *
     * <ul>
     *     <li>When the entire {@link JoinableFile} instance (which manages read and write operations) is closing</li>
     *     <li>When the completing operation locked a file for deletion</li>
     *     <li>When we've just established the first lock on a file, then the operation acquiring this lock fails.</li>
     * </ul>
     *
     * @param f The file whose locks should be cleared
     */
    private void clearLocks( final File f, final String label )
    {
        try
        {
            lockManager.reentrantSynchronous( f.getAbsolutePath(), ( opLock ) -> {
                FileEntry entry = entryMap.get( f.getAbsolutePath() );
                if ( entry != null )
                {
                    logger.trace( "Unlocking {}", f );
                    entry.lockOwner.clearLocks();
                    logger.trace( "Unlocked; clearing resources associated with lock" );

                    closeEntryFile( entry, "" );

                    unlockAssociatedEntries( entry, label );

                    removeEntryAndReentrantLock( entry.name );

                    opLock.signal();
                    logger.trace( "Unlock succeeded." );
                }
                else
                {
                    logger.trace( "{} not locked", f );
                }

                opLock.signal();
                return null;
            } );
        }
        catch ( IOException e )
        {
            logger.error( "IOException trying to unlock: " + f, e );
        }
        catch ( InterruptedException e )
        {
            logger.warn( "Interrupted while trying to unlock: " + f );
        }
    }

    private void closeEntryFile( FileEntry entry, String extraTraceMsg )
    {
        if ( entry.file != null )
        {
            logger.trace( "{} Closing file...", extraTraceMsg == null ? "" : extraTraceMsg );
            IOUtils.closeQuietly( entry.file );
            entry.file = null;
        }
    }

    /**
     * Acquire the given {@link LockLevel} on the specified file, under the provided ownership name and activity label,
     * within the given timeout. This is used to manually lock a file from outside.
     *
     * @param file The file to lock
     * @param label The activity label, to aid in debugging stuck locks
     * @param lockLevel The type of lock to acquire (read, write, delete)
     * @param timeout The timeout period before giving up on the lock acquisition
     * @param unit The time units for the timeout period (milliseconds, etc)
     * @return true if the file was locked as specified, otherwise false
     * @throws InterruptedException
     *
     * @see Partyline#lock(File, long, LockLevel, String)
     * @see LockLevel
     */
    boolean tryLock( File file, String label, LockLevel lockLevel, long timeout, TimeUnit unit )
                    throws InterruptedException
    {
        try
        {
            return tryLock( file, label, lockLevel, timeout, unit, ( opLock ) -> true );
        }
        catch ( IOException e )
        {
            logger.error( "SHOULD NEVER HAPPEN: IOException trying to lock: " + file, e );
        }

        return false;
    }

    /**
     * Acquire the given {@link LockLevel} on the specified file, under the provided ownership name and activity label,
     * within the given timeout. If lock acquisition succeeds, execute the provided operation (normally a lambda).
     * <br/>
     * This method is used within FileTree to handle file lock acquisition before opening / deleting files, among other
     * things.
     * <br/>
     * <b>NOTE:</b> Before attempting to acquire the file lock, this method will acquire the operation semaphore for
     * the given file. This prevents other concurrent calls from overlapping when establishing the first lock on a file,
     * or when releasing the last lock.
     *
     * @param f The file to lock
     * @param label The activity label, to aid in debugging stuck locks
     * @param lockLevel The type of lock to acquire (read, write, delete)
     * @param timeout The timeout period before giving up on the lock acquisition
     * @param unit The time units for the timeout period (milliseconds, etc)
     * @param reentrantOperation The operation to perform once the file lock is acquired
     * @return the result of the provided operation, or else null
     * @throws InterruptedException
     *
     * @see LockLevel
     */
    private <T> T tryLock( File f, String label, LockLevel lockLevel, long timeout, TimeUnit unit,
                           ReentrantOperation<T> reentrantOperation ) throws InterruptedException, IOException
    {
        return lockManager.reentrantSynchronous( f.getAbsolutePath(), ( opLock ) -> {
            long end = timeout < 1 ? -1 : System.currentTimeMillis() + TimeUnit.MILLISECONDS.convert( timeout, unit );

            logger.trace( "{}: Trying to lock until: {}", System.currentTimeMillis(), end );

            String name = f.getAbsolutePath();
            FileEntry entry = null;
            try
            {
                while ( end < 1 || System.currentTimeMillis() < end )
                {
                    entry = getNearestLockingEntry( getUnmodifiableEntryMap(), f );

                    /*
                    There are three basic states we need to capture here:

                    1. The target file is already locked. Try to lock again, and retry / fail as appropriate.

                    2. The target file's ancestor is locked. Try to lock again, and retry / fail as appropriate.
                       When locked, set a flag to tell the system to lock the target file and proceed.

                    3. Neither the target file nor its ancestry is locked. Set a flag to tell the system to lock the
                       target file and proceed.
                     */
                    boolean doFileLock = ( entry == null );

                    if ( !doFileLock )
                    {
                        if ( entry.name.equals( name ) )
                        {
                            if ( entry.lockOwner.lock( label, lockLevel ) )
                            {
                                logger.trace( "Added lock to existing entry: {}", entry.name );
                                try
                                {
                                    return reentrantOperation.execute( opLock );
                                }
                                catch ( IOException | RuntimeException e )
                                {
                                    // we just locked this, and the call failed...reverse the lock operation.
                                    UnlockStatus unlockStatus = entry.lockOwner.unlock( label );
                                    filesystem.updateDominantLocks( entry.file.getPath(), unlockStatus );
                                    throw e;
                                }
                            }
                            else
                            {
                                logger.trace( "Lock failed, but retry may allow another attempt..." );
                            }
                        }
                        else if ( name.startsWith( entry.name ) )
                        {
                            logger.trace( "Re-locking the locking entry: {}.", entry.name );
                            entry.lockOwner.lock( label, lockLevel );

                            FileEntry alsoLocked = entry.alsoLocked;
                            while ( alsoLocked != null )
                            {
                                logger.trace( "ALSO re-locking: {}", alsoLocked.name );
                                alsoLocked.lockOwner.lock( label, read );
                                alsoLocked = alsoLocked.alsoLocked;
                            }

                            doFileLock = true;
                        }
                    }

                    /*
                    If we've been cleared to proceed above, create a new FileEntry instance, lock it, and proceed.
                     */
                    if ( doFileLock )
                    {
                        if ( read == lockLevel && !f.exists() )
                        {
                            throw new IOException( f + " does not exist. Cannot read-lock missing file!" );
                        }

                        entry = new FileEntry( name, label, lockLevel, entry );
                        logger.trace( "No lock on {}; locking as: {} from: {} with also-locked: {}", name, lockLevel,
                                      label, entry.name );
                        entryMap.put( name, entry );
                        try
                        {
                            return reentrantOperation.execute( opLock );
                        }
                        catch ( IOException | RuntimeException e )
                        {
                            // we just locked this, and the call failed...reverse the lock operation.
                            // NOTE: This will CLEAR all locks, which is what we want since there was no FileEntry before.
                            clearLocks( f, label );
                            throw e;
                        }
                    }
                    /*
                    If we haven't succeeded in locking the file (or its ancestry), wait.
                     */
                    else
                    {
                        logger.trace( "Waiting for lock to clear; locking as: {} from: {}", lockLevel, label );
                        opLock.await( WAIT_TIMEOUT );
                    }
                }
            }
            finally
            {
                // no matter what else happens, do NOT allow a delete lock to remain
                if ( entry != null && entry.lockOwner.getLockLevel() == LockLevel.delete && entry.lockOwner.isLocked() )
                {
                    logger.trace( "Clearing locks on delete-locked file entry: {}", f );
                    clearLocks( f, label );
                }
            }

            logger.trace( "{}: {}: Lock failed", System.currentTimeMillis(), name );
            return null;
        } );
    }

    /**
     * Establish a Stream (input or output) associated with a given file. This method will acquire the appropriate lock
     * for the file (using {@link #tryLock(File, String, LockLevel, long, TimeUnit, ReentrantOperation)}) and
     * then retrieve the {@link JoinableFile} instance associated with the file (or create it if necessary). Finally,
     * it passes the JoinableFile to the given {@link JoinableFileOperation} to establish the appropriate stream into / out
     * of that file.
     *
     * @param realFile The file to open
     * @param doOutput If true, the associated function should return an {@link java.io.OutputStream}; else {@link java.io.InputStream}
     * @param timeout The period to wait in attempting to acquire the appropriate file lock
     * @param unit The time unit for the timeout period
     * @param joinableFileOperation The function that establishes the appropriate stream into the {@link JoinableFile}
     * @param <T> The type of stream returned from the given {@link JoinableFileOperation}; output if doOutput is true, else input
     * @return The established stream associated with the given file
     * @throws IOException
     * @throws InterruptedException
     *
     * @see #tryLock(File, String, LockLevel, long, TimeUnit, ReentrantOperation)
     */
    <T> T setOrJoinFile( File realFile, boolean doOutput, long timeout, TimeUnit unit,
                         JoinableFileOperation<T> joinableFileOperation ) throws IOException, InterruptedException
    {
        long end = timeout < 1 ? -1 : System.currentTimeMillis() + TimeUnit.MILLISECONDS.convert( timeout, unit );

        String label = JoinableFile.labelFor( doOutput, Thread.currentThread().getName() );
        while ( end < 1 || System.currentTimeMillis() < end )
        {
            T result = tryLock( realFile, label, doOutput ? LockLevel.write : read, timeout, unit, ( opLock ) -> {
                String path = realFile.getAbsolutePath();
                FileEntry entry = entryMap.get( path );
                boolean proceed = false;
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
                        UnlockStatus unlockStatus = entry.lockOwner.unlock( label );
                        filesystem.updateDominantLocks( entry.file.getPath(), unlockStatus );

                        opLock.signal();

                        logger.trace( "Waiting for file to close at: {}", System.currentTimeMillis() );
                        opLock.await( WAIT_TIMEOUT );

                        logger.trace( "Proceeding with lock attempt at: {} under opLock: {}",
                                      System.currentTimeMillis(), opLock );
                    }
                    else
                    {
                        logger.trace( "Got joinable file" );
                        proceed = true;
                    }
                }
                else
                {
                    logger.trace( "No pre-existing open file; opening new JoinableFile under opLock: {}", opLock );
                    entry.file = filesystem.getFile( realFile, entry.lockOwner,
                                                     new FileTreeCallbacks( entry, realFile, label ), doOutput,
                                                     opLock );

                    proceed = true;
                }

                if ( proceed )
                {
                    return joinableFileOperation.execute( entry.file );
                }

                return null;
            } );

            if ( result != null )
            {
                return result;
            }
        }

        logger.trace( "Failed to lock file for {}", doOutput ? "writing" : "reading" );
        return joinableFileOperation.execute( null );
    }

    /**
     * Attempt to establish a delete lock on the given file, then delete it. Timeout if the specified period expires
     * without delete lock acquisition.
     *
     * @param file The file to lock
     * @param timeout The period to wait for a delete lock
     * @param unit The time unit for the timeout period
     * @return true if the delete lock was successful and the file was force-deleted; false otherwise
     * @throws InterruptedException
     * @throws IOException
     *
     * @see #tryLock(File, String, LockLevel, long, TimeUnit, ReentrantOperation)
     */
    boolean delete( File file, long timeout, TimeUnit unit ) throws InterruptedException, IOException
    {
        return tryLock( file, "Delete File", LockLevel.delete, timeout, unit, ( opLock ) -> {

            if ( globalLockManager != null )
            {
                // for global Evn, we don't mind it is delete or write. We just use write lock for deletion.
                boolean locked = globalLockManager.tryLock( file.getAbsolutePath(), LockLevel.write, -1 );
                if ( !locked )
                {
                    logger.warn( "Can not get global lock and abort deletion, path: ", file.getAbsoluteFile() );
                    return false;
                }
            }

            removeEntryAndReentrantLock( file.getAbsolutePath() );
            opLock.signal();

            if ( file.exists() )
            {
                FileUtils.forceDelete( file );
            }

            if ( globalLockManager != null )
            {
                globalLockManager.unlock( file.getAbsolutePath(), LockLevel.write );
            }
            
            return true;
        } );
    }

    public boolean isLockedByCurrentThread( final File file )
    {
        FileEntry fileEntry = entryMap.get( file.getAbsolutePath() );
        return fileEntry != null && fileEntry.lockOwner.isLockedByCurrentThread();
    }

    /**
     * Class which manages the state associated with files and {@link JoinableFile}s in partyline. These keep the lock
     * associated with a path and a {@link JoinableFile}, even when there is no JoinableFile yet. They are mapped to the
     * path to allow concurrent operations to access this state and open additional (reader) streams, etc.
     */
    public static final class FileEntry
    {
        private final String name;

        private FileEntry alsoLocked;

        private final LocalLockOwner lockOwner;

        private JoinableFile file;

        public LocalLockOwner getLockOwner()
        {
            return lockOwner;
        }

        public JoinableFile getFile()
        {
            return file;
        }

        FileEntry( String name, String lockingLabel, LockLevel lockLevel, final FileEntry alsoLocked )
        {
            this.name = name;
            this.alsoLocked = alsoLocked;
            this.lockOwner = new LocalLockOwner( name, lockingLabel, lockLevel );
        }
    }

    /**
     * {@link StreamCallbacks} implementation which can wrap another instance passed into {@link FileTree} operations,
     * and which takes care of clearing all locks on a file when the {@link JoinableFile} is finally closed.
     */
    private final class FileTreeCallbacks
                    implements StreamCallbacks
    {
        private File file;

        private FileEntry entry;

        private String label;

        public FileTreeCallbacks( FileEntry entry, File file, final String label )
        {
            this.file = file;
            this.entry = entry;
            this.label = label;
        }

        @Override
        public void flushed()
        {
        }

        @Override
        public void beforeClose()
        {
        }

        @Override
        public void closed()
        {
            logger.trace( "unlocking: {}", file );

            // already inside lock from JoinableFile.reallyClose().
            entry.file = null;

            // the whole JoinableFile is closing. Clear remaining locks.
            clearLocks( file, label );
        }
    }

    /**
     * Operation that returns a stream (InputStream or OutputStream) from a {@link JoinableFile}. This is used from
     * {@link Partyline#openInputStream(File, long)} and {@link Partyline#openOutputStream(File, long)}
     * via {@link FileTree#setOrJoinFile(File, boolean, long, TimeUnit, JoinableFileOperation)}.
     *
     * @param <T> The stream result.
     *
     * @see Partyline#openInputStream(File, long)
     * @see Partyline#openOutputStream(File, long)
     * @see FileTree#setOrJoinFile(File, boolean, long, TimeUnit, JoinableFileOperation)
     */
    @FunctionalInterface
    interface JoinableFileOperation<T>
    {
        T execute( JoinableFile file ) throws IOException;
    }
}
