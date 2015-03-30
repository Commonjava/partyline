/*******************************************************************************
* Copyright (c) 2015 Red Hat, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the GNU Public License v3.0
* which accompanies this distribution, and is available at
* http://www.gnu.org/licenses/gpl.html
*
* Contributors:
* Red Hat, Inc. - initial API and implementation
******************************************************************************/
package org.commonjava.util.partyline;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.commonjava.util.partyline.callback.AbstractStreamCallbacks;
import org.commonjava.util.partyline.callback.CallbackInputStream;
import org.commonjava.util.partyline.callback.CallbackOutputStream;

/**
 * File manager that attempts to manage read/write locks in the presence of output streams that will allow simultaneous access to read the content
 * they are writing. Also allows the user to lock/unlock files manually in case they need to be used outside the normal streaming use cases.
 * 
 * @author jdcasey
 */
public class JoinableFileManager
{

    private final Map<File, JoinableOutputStream> joinableStreams = new HashMap<>();

    private final Set<File> activeFiles = new HashSet<>();
    
    private final Set<File> manualLocks = new HashSet<>();

    /**
     * If the file isn't marked as active, create a new {@link JoinableOutputStream} to the specified file and pass it back to the user.
     */
    public OutputStream openOutputStream( final File file )
        throws IOException
    {
        synchronized ( activeFiles )
        {
            if ( !waitForFile( file ) )
            {
                return null;
            }

            activeFiles.add( file );
            activeFiles.notifyAll();
        }

        final JoinableOutputStream out = new JoinableOutputStream( file );
        joinableStreams.put( file, out );

        return new CallbackOutputStream( out, new StreamCallback( file, activeFiles ) );
    }
    
    /**
     * If there is an active {@link JoinableOutputStream}, call {@link JoinableOutputStream#joinStream()} and return it to the user. Otherwise, open
     * a new {@link FileInputStream} to the specified file, wrap it in a {@link CallbackInputStream} to notify this manager when it closes, and pass
     * the result back to the user.
     */
    public InputStream openInputStream( final File file )
        throws FileNotFoundException, IOException
    {
        final JoinableOutputStream joinable = joinableStreams.get( file );
        if ( joinable != null )
        {
            return joinable.joinStream();
        }
        else
        {
            synchronized ( activeFiles )
            {
                if ( !waitForFile( file ) )
                {
                    return null;
                }

                activeFiles.add( file );
                activeFiles.notifyAll();
            }

            return new CallbackInputStream( new FileInputStream( file ), new StreamCallback( file, activeFiles ) );
        }
    }

    /**
     * Manually lock the specified file to prevent opening any streams via this manager (until manually unlocked).
     */
    public boolean lock( final File file )
    {
        synchronized ( activeFiles )
        {
            if ( activeFiles.contains( file ) )
            {
                return false;
            }

            activeFiles.add( file );
            activeFiles.notifyAll();
            
            manualLocks.add( file );
        }

        return true;
    }

    /**
     * If the specified file was manually locked, unlock it and return true. Otherwise, return false.
     */
    public boolean unlock( final File file )
    {
        synchronized ( activeFiles )
        {
            if ( !manualLocks.remove( file ) )
            {
                return false;
            }

            return activeFiles.remove( file );
        }
    }
    
    /**
     * Check if the specified file is locked against write operations. Files are write-locked if any other file access is active.
     */
    public boolean isWriteLocked( final File file )
    {
        return activeFiles.contains( file );
    }

    /**
     * Check if the specified file is locked against read operations. This is only the case if the file is active AND not in use via a 
     * {@link JoinableOutputStream}, since joinable streams allow multiple reads while the write operation is ongoing.
     */
    public boolean isReadLocked( final File file )
    {
        return activeFiles.contains( file ) && !joinableStreams.containsKey( file );
    }

    /**
     * Wait the specified timeout milliseconds for write access on the specified file to become available. Return false if the timeout elapses without
     * the file becoming available for writes.
     * 
     * @see #isWriteLocked(File)
     */
    public boolean waitForWriteUnlock( final File file, final long timeout )
    {
        synchronized ( activeFiles )
        {
            return waitForFile( file, timeout );
        }
    }

    /**
     * Wait the specified timeout milliseconds for read access on the specified file to become available. Return false if the timeout elapses without
     * the file becoming available for reads. If a {@link JoinableOutputStream} is available for the file, don't wait (immediately return true).
     * 
     * @see #isReadLocked(File)
     */
    public boolean waitForReadUnlock( final File file, final long timeout )
    {
        if ( joinableStreams.containsKey( file ) )
        {
            return true;
        }

        synchronized ( activeFiles )
        {
            return waitForFile( file, timeout );
        }
    }

    private boolean waitForFile( final File file )
    {

        return waitForFile( file, -1 );
    }

    private boolean waitForFile( final File file, final long timeout )
    {
        boolean proceed = true;
        try
        {
            if ( timeout < 0 )
            {
                wait();
            }
            else
            {
                wait( timeout );
                proceed = !activeFiles.contains( file );
            }
        }
        catch ( final InterruptedException e )
        {
            Thread.currentThread()
                  .interrupt();
            proceed = false;
        }

        return proceed;
    }

    private static final class StreamCallback
        extends AbstractStreamCallbacks
    {
        private final File file;

        private final Set<File> active;

        StreamCallback( final File file, final Set<File> active )
        {
            this.file = file;
            this.active = active;
        }

        @Override
        public void closed()
        {
            synchronized ( active )
            {
                if ( active.remove( file ) )
                {
                    active.notifyAll();
                }
            }
        }
    }

}
