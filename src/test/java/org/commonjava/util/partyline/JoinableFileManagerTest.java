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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.commonjava.util.partyline.fixture.AbstractJointedIOTest;
import org.commonjava.util.partyline.fixture.TimedTask;
import org.junit.Assert;
import org.junit.Test;

public class JoinableFileManagerTest
    extends AbstractJointedIOTest
{

    private static final long SHORT_TIMEOUT = 10;

    private final JoinableFileManager mgr = new JoinableFileManager();

    @Test
    public void openOutputStream_VerifyWriteLocked_NotReadLocked()
        throws Exception
    {
        final File f = temp.newFile();
        final OutputStream stream = mgr.openOutputStream( f );

        assertThat( mgr.isWriteLocked( f ), equalTo( true ) );
        assertThat( mgr.isReadLocked( f ), equalTo( false ) );

        stream.close();

        assertThat( mgr.isWriteLocked( f ), equalTo( false ) );
        assertThat( mgr.isReadLocked( f ), equalTo( false ) );
    }

    @Test
    public void openOutputStream_TimeBoxedSecondCallReturnsNull()
        throws Exception
    {
        final File f = temp.newFile();
        final OutputStream stream = mgr.openOutputStream( f );

        OutputStream s2 = mgr.openOutputStream( f, SHORT_TIMEOUT );

        assertThat( s2, nullValue() );

        stream.close();

        s2 = mgr.openOutputStream( f, SHORT_TIMEOUT );

        assertThat( s2, notNullValue() );
    }

    @Test
    public void openOutputStream_SecondWaitsUntilFirstCloses()
        throws Exception
    {
        final File f = temp.newFile();

        final String first = "first";
        final String second = "second";

        final OpenOutputStream secondRunnable = new OpenOutputStream( f, -1 );
        final Map<String, Long> timings =
            testTimings( new TimedTask( first, new OpenOutputStream( f, SHORT_TIMEOUT ) ),
                         new TimedTask( second, secondRunnable ) );

        System.out.println( first + " completed at: " + timings.get( first ) );
        System.out.println( second + " completed at: " + timings.get( second ) );

        IOUtils.closeQuietly( secondRunnable );
        assertThat( timings.get( first ) < timings.get( second ), equalTo( true ) );
    }

    @Test
    public void openInputStream_VerifyWriteLocked_ReadLocked()
        throws Exception
    {
        final File f = temp.newFile();
        final InputStream stream = mgr.openInputStream( f );

        assertThat( mgr.isWriteLocked( f ), equalTo( true ) );
        assertThat( mgr.isReadLocked( f ), equalTo( true ) );

        stream.close();

        assertThat( mgr.isWriteLocked( f ), equalTo( false ) );
        assertThat( mgr.isReadLocked( f ), equalTo( false ) );
    }

    @Test
    public void openInputStream_TimeBoxedSecondCallReturnsNull()
        throws Exception
    {
        final File f = temp.newFile();
        final InputStream stream = mgr.openInputStream( f );

        InputStream s2 = mgr.openInputStream( f, SHORT_TIMEOUT );

        assertThat( s2, nullValue() );

        stream.close();

        s2 = mgr.openInputStream( f, SHORT_TIMEOUT );

        assertThat( s2, notNullValue() );
    }

    @Test
    public void lockFile_OpenOutputStreamWaitsForUnlock()
        throws Exception
    {
        final File f = temp.newFile();
        final OpenOutputStream outputRunnable = new OpenOutputStream( f, -1 );

        final String lockUnlock = "lock-unlock";
        final String output = "output";

        final Map<String, Long> timings =
            testTimings( new TimedTask( lockUnlock, new LockThenUnlockFile( f, 100 ) ), new TimedTask( output,
                                                                                                      outputRunnable ) );

        IOUtils.closeQuietly( outputRunnable );

        System.out.println( "Lock-Unlock completed at: " + timings.get( lockUnlock ) );
        System.out.println( "OpenOutputStream completed at: " + timings.get( output ) );

        assertThat( timings.get( lockUnlock ) < timings.get( output ), equalTo( true ) );
    }

    private final class OpenOutputStream
        implements Runnable, Closeable
    {
        private final long timeout;

        private final File file;

        private OutputStream stream;

        public OpenOutputStream( final File file, final long timeout )
        {
            this.file = file;
            this.timeout = timeout;
        }

        @Override
        public void run()
        {
            try
            {
                //                System.out.println( Thread.currentThread()
                //                                          .getName() + ": Opening output stream: " + file );
                stream = mgr.openOutputStream( file );
                //                System.out.println( Thread.currentThread()
                //                                          .getName() + ": Opened output stream: " + file );
            }
            catch ( final IOException e )
            {
                e.printStackTrace();
                Assert.fail( "Failed to open stream: " + e.getMessage() );
            }

            if ( stream != null && timeout > -1 )
            {
                if ( timeout > 0 )
                {
                    try
                    {
                        //                        System.out.println( Thread.currentThread()
                        //                                                  .getName() + ": Waiting " + timeout + "ms" );
                        Thread.sleep( timeout );
                    }
                    catch ( final InterruptedException e )
                    {
                        Assert.fail( "Interrupted!" );
                    }
                }

                IOUtils.closeQuietly( stream );
            }
        }

        @Override
        public void close()
            throws IOException
        {
            if ( stream != null )
            {
                stream.close();
            }
        }
    }

    private final class LockThenUnlockFile
        implements Runnable
    {
        private final long timeout;

        private final File file;

        public LockThenUnlockFile( final File file, final long timeout )
        {
            this.file = file;
            this.timeout = timeout;
        }

        @Override
        public void run()
        {
            //            System.out.println( Thread.currentThread()
            //                                      .getName() + ": Locking: " + file );
            final boolean locked = mgr.lock( file );
            //            System.out.println( Thread.currentThread()
            //                                      .getName() + ": Locked: " + file + "? " + locked );
            assertThat( locked, equalTo( true ) );

            try
            {
                //                System.out.println( Thread.currentThread()
                //                                          .getName() + ": Waiting for unlock: " + timeout + "ms" );
                Thread.sleep( timeout );
            }
            catch ( final InterruptedException e )
            {
                Assert.fail( "Interrupted!" );
            }

            //            System.out.println( Thread.currentThread()
            //                                      .getName() + ": Unlocking: " + file );
            assertThat( mgr.unlock( file ), equalTo( true ) );
            //            System.out.println( Thread.currentThread()
            //                                      .getName() + ": Unlocked: " + file );
        }
    }

}
