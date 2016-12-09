package org.commonjava.util.partyline;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Locks a single operation on a File in this FileTree, so competing operations ON THAT FILE have to wait, but
 * operations on other files can continue.
 */
final class FileOperationLock
{
    private ReentrantLock lock = new ReentrantLock();

    private Condition changed = lock.newCondition();

    public void lock()
    {
        lock.lock();
    }

    public void unlock()
    {
        lock.unlock();
    }

    public void await( long timeoutMs )
            throws InterruptedException
    {
        changed.await( timeoutMs, TimeUnit.MILLISECONDS );
    }

    public void signal()
    {
        changed.signal();
    }

    public <T> T lockAnd( LockedFileOperation<T> op )
            throws IOException, InterruptedException
    {
        try
        {
            lock();

            return op.execute( this );
        }
        finally
        {
            unlock();
        }
    }

}
