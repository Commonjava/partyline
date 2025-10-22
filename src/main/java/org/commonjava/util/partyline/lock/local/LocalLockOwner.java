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
package org.commonjava.util.partyline.lock.local;

import org.commonjava.cdi.util.weft.ThreadContext;
import org.commonjava.util.partyline.lock.LockLevel;
import org.commonjava.util.partyline.lock.UnlockStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Maintain information about threads with active locks on a file, along the current lock-level of the file (which
 * determines what additional operations can be added, once the initial operation is started). This class counts
 * referents that have locked a file, to determine when a file is completely unlocked (and could be re-locked for
 * operations that would have been forbidden previously, like deletion).
 *
 * @see LockLevel for more information about allowable operations for given lock levels
 */
public final class LocalLockOwner
{

    public static final String PARTYLINE_LOCK_OWNER = "partyline-lock-owner";

    private final Logger logger = LoggerFactory.getLogger( getClass() );

    private final Map<String, LockOwnerInfo> lockInfoMap = new LinkedHashMap<>();

    private String path;

    private LockLevel dominantLockLevel;

    private String dominantOwner;

    public LocalLockOwner( String path, String label, LockLevel lockLevel )
    {
        this.path = path;
        this.dominantLockLevel = lockLevel;
        this.dominantOwner = getLockReservationName();
        increment( label, lockLevel );
    }

    public boolean isLocked()
    {
        return !lockInfoMap.isEmpty();
    }

    public boolean isLockedByCurrentThread()
    {
        return !lockInfoMap.isEmpty() && lockInfoMap.containsKey( getLockReservationName() );
    }

    public synchronized boolean lock( String label, LockLevel lockLevel )
    {
        String lockOwner = getLockReservationName();
        if ( lockInfoMap.isEmpty() )
        {
            logger.trace( "Not locked; locking: {}", lockOwner );
            this.dominantLockLevel = lockLevel;
            this.dominantOwner = lockOwner;
            increment( label, lockLevel );
            return true;
        }

        LockOwnerInfo ownerInfo = lockInfoMap.get( lockOwner );
        if ( ownerInfo != null && ownerInfo.level == lockLevel )
        {
            increment(label, lockLevel);
            return true;
        }

        switch ( lockLevel )
        {
            case delete:
            case write:
            {
                logger.trace( "[ABORT] Trying to lock at level: {} from owner: {}. Existing lock is: {}", lockLevel,
                              lockOwner, this.dominantLockLevel );
                return false;
            }
            case read:
            {
                if ( this.dominantLockLevel == LockLevel.delete )
                {
                    logger.trace( "Already locked at delete level. Ignoring: {}", label );
                    return false;
                }

                increment( label, lockLevel );
                return true;
            }
            default:
                return false;
        }
    }

    @Override
    public String toString()
    {
        return String.format( "LockOwner [%s] of: %s", super.hashCode(), path );
    }

    public synchronized CharSequence getLockInfo()
    {
        StringBuilder sb = new StringBuilder().append( "Lock level: " )
                                  .append( dominantLockLevel )
                                  .append( "\nLocks: " );
        lockInfoMap.values().forEach( ( v ) -> {
            sb.append(v.ownerName).append(":\n  ");
            v.labels.forEach( ( label ) -> sb.append( label ).append( '\n' ) );
        } );

        return sb;
    }

    private synchronized int increment( String label, LockLevel level )
    {
        String ownerName = getLockReservationName();
        LockOwnerInfo lockOwnerInfo = lockInfoMap.computeIfAbsent( ownerName, o->new LockOwnerInfo( level ) );

        lockOwnerInfo.labels.add( label );
        int lockCount = lockOwnerInfo.labels.size();

        logger.trace( "\n\n\n{}\n  Incremented lock count.\n  New count is: {} \n  Owner: {}\n  Ref: {}\n\n\n", path, lockCount, ownerName, label );
        return lockCount;
    }

    public synchronized UnlockStatus unlock( final String label )
    {
        String ownerName = getLockReservationName();
        LockOwnerInfo lockOwnerInfo = lockInfoMap.get( ownerName );
        if ( lockOwnerInfo == null )
        {
            logger.trace( "Not locked by: {}. Returning null.", ownerName );
            return new UnlockStatus( false, false, null );
        }

        lockOwnerInfo.labels.remove( label );
        int count = lockOwnerInfo.labels.size();
        logger.trace( "Decremented lock count.\n  Path: {}\n  for owner: {}\n  New count is: {}\nLock Info:\n{}", this.path, ownerName, count, getLockInfo() );

        if ( count < 1 )
        {
            lockInfoMap.remove( ownerName );
            if ( dominantOwner.equals( ownerName ) )
            {
                logger.trace( "Unlocked owner is removed, but was dominant lock holder. Calculating new dominant lock holder." );

                Optional<LockOwnerInfo> first = lockInfoMap.values()
                                                           .stream()
                                                           .sorted( ( o1, o2 ) -> new Integer( o2.level.ordinal() ).compareTo(
                                                             o1.level.ordinal() ) )
                                                           .findFirst();

                if ( first.isPresent() )
                {
                    LockOwnerInfo newDom = first.get();
                    this.dominantOwner = newDom.ownerName;
                    // check to see if the dominant lock level actually changed
                    boolean changed = ( this.dominantLockLevel != newDom.level );
                    this.dominantLockLevel = newDom.level;
                    logger.trace( "New dominant holder is: {} with level: {}", this.dominantOwner,
                                  this.dominantLockLevel );
                    return new UnlockStatus( true, changed, this.dominantLockLevel );
                }
                else
                {
                    logger.trace( "Locks seems to be empty; Unlocking" );
                    this.dominantOwner = null;
                    this.dominantLockLevel = null;
                    return new UnlockStatus( true, true, null );
                }
            }

            return new UnlockStatus( true, false, this.dominantLockLevel );
        }

        logger.trace( "Unlock operation did not free final lock from file" );
        return new UnlockStatus( false, false, this.dominantLockLevel );
    }

    public LockLevel getLockLevel()
    {
        return dominantLockLevel;
    }

    public synchronized int getContextLockCount()
    {
        String ownerName = getLockReservationName();
        LockOwnerInfo lockOwnerInfo = lockInfoMap.get( ownerName );

        return lockOwnerInfo == null ? 0 : lockOwnerInfo.labels.size();
    }

    public synchronized void clearLocks()
    {
        lockInfoMap.clear();
        this.dominantLockLevel = null;
        this.dominantOwner = null;
    }

    public static String getLockReservationName()
    {
        ThreadContext ctx = ThreadContext.getContext( true );
        String ownerName = (String) ctx.get( PARTYLINE_LOCK_OWNER );
        if ( ownerName == null )
        {
            ownerName = "Context of: " + Thread.currentThread().getName();
            ctx.put( PARTYLINE_LOCK_OWNER, ownerName );
        }

        return ownerName;
    }

    private static final class LockOwnerInfo
    {
        private String ownerName = getLockReservationName();

        private List<String> labels = Collections.synchronizedList( new ArrayList<>() );

        private LockLevel level;

        LockOwnerInfo( LockLevel level )
        {
            this.level = level;
        }

        @Override
        public String toString()
        {
            return "LockOwnerInfo{" + "ownerName='" + ownerName + '\'' + ", labels=" + labels + ", level=" + level
                            + '}';
        }
    }

}
