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

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.commonjava.util.partyline.fixture.AbstractJointedIOTest;
import org.commonjava.util.partyline.fixture.TimedTask;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    @Test( expected = IOException.class )
    public void openOutputStream_TimeBoxedSecondCallThrowsException()
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
            testTimings( 10, new TimedTask( first, new OpenOutputStream( f, 100 ) ), new TimedTask( second,
                                                                                                   secondRunnable ) );

        System.out.println( first + " completed at: " + timings.get( first ) );
        System.out.println( second + " completed at: " + timings.get( second ) );

        assertThat( first + " completed at: " + timings.get( first ) + "\n" + second + " completed at: "
                        + timings.get( second ) + "\nFirst should complete before second.",
                    timings.get( first ) < timings.get( second ), equalTo( true ) );
    }

    @Test
    public void verifyReportingDaemonWorks()
        throws Exception
    {
        final File f = temp.newFile();

        final String first = "first";
        final String second = "second";

        mgr.startReporting( 0, 1000 );

        final OpenOutputStream secondRunnable = new OpenOutputStream( f, -1 );
        final Map<String, Long> timings = testTimings( new TimedTask( first, new OpenOutputStream( f, 10000 ) ) );

        System.out.println( first + " completed at: " + timings.get( first ) );
        System.out.println( second + " completed at: " + timings.get( second ) );
    }

    @Test
    public void openInputStream_VerifyWriteLocked_ReadLocked()
        throws Exception
    {
        final File f = temp.newFile();
        final InputStream stream = mgr.openInputStream( f );

        assertThat( mgr.isWriteLocked( f ), equalTo( true ) );

        // after change to always use JoinableFile (even for read-first), I don't think partyline will ever read lock.
        assertThat( mgr.isReadLocked( f ), equalTo( false ) );

        stream.close();

        assertThat( mgr.isWriteLocked( f ), equalTo( false ) );
        assertThat( mgr.isReadLocked( f ), equalTo( false ) );
    }

    @Test
    @Ignore( "With change to JoinableFile for all reads, partyline should never read-lock" )
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
    public void openInputStream_ConcurrentReadersGetSameResult()
            throws Exception
    {
        final File f = temp.newFile();
        String str = "This is a test";
        FileUtils.write( f, str );
        InputStream s1 = null;
        InputStream s2 = null;
        try
        {
            s1 = mgr.openInputStream( f );
            s2 = mgr.openInputStream( f, SHORT_TIMEOUT );

            String out1 = IOUtils.toString( s1 );
            String out2 = IOUtils.toString( s2 );

            assertThat( "first reader returned wrong data", out1, equalTo( str ) );
            assertThat( "second reader returned wrong data", out2, equalTo( str ) );
        }
        finally
        {
            s1.close();
            s2.close();
        }
    }

    @Test
    public void lockFile_OpenOutputStreamWaitsForUnlock()
        throws Exception
    {
        final File f = temp.newFile("test.txt");
        final OpenOutputStream outputRunnable = new OpenOutputStream( f, -1 );

        final String lockUnlock = "lock-unlock";
        final String output = "output";

        final Map<String, Long> timings =
            testTimings( 200, new TimedTask( lockUnlock, new LockThenUnlockFile( f, 100 ) ),
                         new TimedTask( output, outputRunnable ) );

        System.out.println( "Lock-Unlock completed at: " + timings.get( lockUnlock ) );
        System.out.println( "OpenOutputStream completed at: " + timings.get( output ) );

        assertThat( "\nLock-Unlock completed at:             " + timings.get( lockUnlock ) + "\n"
                        + "OpenOutputStream completed at: " + timings.get( output )
                        + "\nLock-Unlock should complete first",
                    timings.get( lockUnlock ) < timings.get( output ), equalTo( true ) );
    }

    private final class OpenOutputStream
        implements Runnable
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
            Logger logger = LoggerFactory.getLogger( getClass() );
            try
            {
                logger.trace( "locking: {}", file );
                final boolean locked = mgr.lock( file, 100, true );

                logger.trace( "locked? {}", locked );

                assertThat( locked, equalTo( true ) );

                logger.trace( "Waiting {}ms to unlock...", timeout );
                Thread.sleep( timeout );
            }
            catch ( final InterruptedException e )
            {
                Assert.fail( "Interrupted!" );
            }
            catch ( IOException e )
            {
                e.printStackTrace();
                Assert.fail( "Failed to acquire lock on: " + file );
            }

            logger.trace( "unlocking: {}", file );
            assertThat( mgr.unlock( file ), equalTo( true ) );
        }
    }

}
