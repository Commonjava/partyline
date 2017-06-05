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

import org.commonjava.cdi.util.weft.ThreadContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.commons.lang.StringUtils.join;

/**
 * Maintain information about threads with active locks on a file, along the current lock-level of the file (which
 * determines what additional operations can be added, once the initial operation is started). This class counts
 * referents that have locked a file, to determine when a file is completely unlocked (and could be re-locked for
 * operations that would have been forbidden previously, like deletion).
 *
 * @see LockLevel for more information about allowable operations for given lock levels
 */
final class LockOwner
{

    public static final String PARTYLINE_LOCK_OWNER = "partyline-lock-owner";

    private final Logger logger = LoggerFactory.getLogger( getClass() );

    private final Map<String, LockOwnerInfo> locks = new LinkedHashMap<>();

    private String path;

    private LockLevel dominantLockLevel;

    private String dominantOwner;

    LockOwner( String path, String label, LockLevel lockLevel )
    {
        this.path = path;
        this.dominantLockLevel = lockLevel;
        this.dominantOwner = getLockReservationName();
        increment( label, lockLevel );
    }

    boolean isLocked()
    {
        return !locks.isEmpty();
    }

    boolean isLockedByCurrentThread()
    {
        return !locks.isEmpty() && locks.containsKey( getLockReservationName() );
    }

    synchronized boolean lock( String label, LockLevel lockLevel )
    {
        String lockOwner = getLockReservationName();
        if ( locks.isEmpty() )
        {
            logger.trace( "Not locked; locking: {}", lockOwner );
            this.dominantLockLevel = lockLevel;
            this.dominantOwner = lockOwner;
            increment( label, lockLevel );
            return true;
        }

        switch ( lockLevel )
        {
            case delete:
            case write:
            {
                logger.trace( "[ABORT] Trying to lock at level: {} from owner: {}. Existing lock is: {}", lockLevel, lockOwner, this.dominantLockLevel );
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

    synchronized CharSequence getLockInfo()
    {
        return new StringBuilder().append( "Lock level: " )
                                  .append( dominantLockLevel )
                                  .append( "\nOwner context is: " )
                                  .append( locks.entrySet() );
    }

    private synchronized int increment( String label, LockLevel level )
    {
        String ownerName = getLockReservationName();
        LockOwnerInfo lockOwnerInfo = locks.computeIfAbsent( ownerName, o->new LockOwnerInfo( level ) );

        int lockCount = lockOwnerInfo.locks.incrementAndGet();

        Logger logger = LoggerFactory.getLogger( getClass() );

        logger.trace( "{} Incremented lock count to: {} for owner: {} with ref: {}", this, lockCount, ownerName, label );
        return lockCount;
    }

    synchronized boolean unlock()
    {
        String ownerName = getLockReservationName();
        LockOwnerInfo lockOwnerInfo = locks.get( ownerName );
        if ( lockOwnerInfo == null )
        {
            logger.trace( "Not locked by: {}. Returning false.", ownerName );
            return false;
        }

        int count = lockOwnerInfo.locks.decrementAndGet();
        logger.trace( "Decremented lock count in: {} for owner: {}. New count is: {}\nLock Info:\n{}", this.path, ownerName, count, getLockInfo() );

        if ( count < 1 )
        {
            locks.remove( ownerName );
            if ( dominantOwner.equals( ownerName ) )
            {
                logger.trace( "Unlocked owner is removed, but was dominant lock holder. Calculating new dominant lock holder." );

                Optional<LockOwnerInfo> first = locks.values()
                                                     .stream()
                                                     .sorted( ( o1, o2 ) -> new Integer( o2.level.ordinal() ).compareTo(
                                                             o1.level.ordinal() ) )
                                                     .findFirst();

                if ( first.isPresent() )
                {
                    LockOwnerInfo newDom = first.get();
                    this.dominantOwner = newDom.ownerName;
                    this.dominantLockLevel = newDom.level;
                    logger.trace( "New dominant holder is: {} with level: {}", this.dominantOwner,
                                  this.dominantLockLevel );
                }
                else
                {
                    logger.trace( "Locks seems to be empty; Unlocking" );
                    this.dominantOwner = null;
                    this.dominantLockLevel = null;
                }
            }

            return true;
        }

        logger.trace( "Unlock operation did not free final lock from file" );
        return false;
    }

    LockLevel getLockLevel()
    {
        return dominantLockLevel;
    }

    synchronized int getLockTimes()
    {
        String ownerName = getLockReservationName();
        LockOwnerInfo lockOwnerInfo = locks.get( ownerName );

        return lockOwnerInfo == null ? 0 : lockOwnerInfo.locks.get();
    }

    synchronized void clearLocks()
    {
        locks.clear();
        this.dominantLockLevel = null;
        this.dominantOwner = null;
    }

    public static String getLockReservationName()
    {
        ThreadContext ctx = ThreadContext.getContext( true );
        String ownerName = (String) ctx.get( PARTYLINE_LOCK_OWNER );
        if ( ownerName == null )
        {
            ownerName = "Threads sharing context with: " + Thread.currentThread().getName();
            ctx.put( PARTYLINE_LOCK_OWNER, ownerName );
        }

        return ownerName;
    }

    private static final class LockOwnerInfo
    {
        private String ownerName = getLockReservationName();
        private AtomicInteger locks = new AtomicInteger( 0 );
        private LockLevel level;

        LockOwnerInfo( LockLevel level )
        {
            this.level = level;
        }

        @Override
        public String toString()
        {
            return "LockOwnerInfo{" + "ownerName='" + ownerName + '\'' + ", locks=" + locks + ", level=" + level + '}';
        }
    }

}
