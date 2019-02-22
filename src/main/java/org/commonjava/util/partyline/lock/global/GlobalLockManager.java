package org.commonjava.util.partyline.lock.global;

import org.commonjava.util.partyline.PartylineException;
import org.commonjava.util.partyline.lock.LockLevel;

/**
 * GlobalLockManager is for cluster Env.
 *
 * Each node needs to get a lock before reading or writing to a path and unlock it once finish r/w operations.
 * When a path is r-locked by a node, other nodes can add r-lock to it, but w-lock is forbidden.
 * When a path is w-locked by a node, other nodes can not add any further locks.
 *
 * This does not affect threads on local node. All the threads can still join the reading when one thread is writing.
 */
public interface GlobalLockManager
{
    /**
     * Try to lock a path.
     * @param path
     * @param level
     * @param timeoutInMillis
     * @return true if succeeded. false if failed to lock during the timeout limit.
     */
    boolean tryLock( String path, LockLevel level, long timeoutInMillis ) throws PartylineException;

    /**
     * Unlock a path.
     * @param path
     * @param level
     */
    void unlock( String path, LockLevel level ) throws PartylineException;
}
