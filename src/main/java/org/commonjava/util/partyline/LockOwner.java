package org.commonjava.util.partyline;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.ref.WeakReference;

public class LockOwner
    implements Closeable
{

    public enum LockType
    {
        INPUT, JOINABLE_OUTPUT, MANUAL;
    }

    private final WeakReference<Thread> threadRef;

    private final long threadId;

    private final String threadName;

    private final StackTraceElement[] lockOrigin;

    private InputStream inputStream;

    private OutputStream outputStream;

    private LockType lockType;

    public LockOwner( final OutputStream outputStream )
    {
        this.outputStream = outputStream;
        final Thread t = Thread.currentThread();
        this.threadRef = new WeakReference<Thread>( t );
        this.threadName = t.getName();
        this.threadId = t.getId();
        this.lockOrigin = t.getStackTrace();
        this.lockType = LockType.JOINABLE_OUTPUT;
    }

    public LockOwner( final InputStream inputStream )
    {
        this.inputStream = inputStream;
        final Thread t = Thread.currentThread();
        this.threadRef = new WeakReference<Thread>( t );
        this.threadName = t.getName();
        this.threadId = t.getId();
        this.lockOrigin = t.getStackTrace();
        this.lockType = LockType.INPUT;
    }

    public LockOwner()
    {
        this( (InputStream) null );
        this.lockType = LockType.MANUAL;
    }

    public boolean isAlive()
    {
        return threadRef.get() != null && threadRef.get()
                                                   .isAlive();
    }

    public long getThreadId()
    {
        return threadId;
    }

    public String getThreadName()
    {
        return threadName;
    }

    public StackTraceElement[] getLockOrigin()
    {
        return lockOrigin;
    }

    public Thread getThread()
    {
        return threadRef.get();
    }

    @Override
    public void close()
        throws IOException
    {
        if ( inputStream != null )
        {
            inputStream.close();
        }
        else if ( outputStream != null )
        {
            outputStream.close();
        }
    }

    public LockType getLockType()
    {
        return lockType;
    }

}
