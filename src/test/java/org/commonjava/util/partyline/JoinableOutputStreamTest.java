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
import static org.junit.Assert.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

public class JoinableOutputStreamTest
{
    private static final int COUNT = 2000;

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    @Rule
    public TestName name = new TestName();

    private int readers = 0;

    @Test
    public void writeToFile()
        throws Exception
    {
        final CountDownLatch latch = new CountDownLatch( 1 );
        final JoinableOutputStream stream = startTimedWrite( 0, latch );

        System.out.println( "Waiting for " + name.getMethodName() + " threads to complete." );
        latch.await();

        final File file = stream.getFile();

        System.out.println( "File length: " + file.length() );

        final List<String> lines = FileUtils.readLines( file );
        System.out.println( lines );

        assertThat( lines.size(), equalTo( COUNT ) );
    }

    @Test
    public void writeToFileInPresenceOfSlowRawRead()
        throws Exception
    {
        final File file = temp.newFile();
        final List<String> lines = new ArrayList<>();
        for ( int i = 0; i < COUNT; i++ )
        {
            lines.add( ">" + Integer.toString( i ) );
        }

        FileUtils.writeLines( file, lines );

        System.out.println( "Original file length: " + file.length() );

        final CountDownLatch latch = new CountDownLatch( 2 );
        startTimedRawRead( file, 0, 1, -1, latch );

        final JoinableOutputStream stream = startTimedWrite( file, 1, latch );

        System.out.println( "Waiting for " + name.getMethodName() + " threads to complete." );
        latch.await();

        System.out.println( "File length: " + file.length() );

        final List<String> result = FileUtils.readLines( file );
        System.out.println( result );

        assertThat( result.size(), equalTo( COUNT ) );
    }

    @Test
    public void joinFileWrite()
        throws Exception
    {
        final CountDownLatch latch = new CountDownLatch( 2 );
        final JoinableOutputStream stream = startTimedWrite( 0, latch );
        startRead( 0, stream, latch );

        System.out.println( "Waiting for " + name.getMethodName() + " threads to complete." );
        latch.await();
    }

    @Test
    public void joinFileWriteTwiceWithDelay()
        throws Exception
    {
        final CountDownLatch latch = new CountDownLatch( 3 );
        final JoinableOutputStream stream = startTimedWrite( 1, latch );
        startRead( 0, stream, latch );
        startRead( 500, stream, latch );

        System.out.println( "Waiting for " + name.getMethodName() + " threads to complete." );
        latch.await();
    }

    @Test
    public void joinFileWriteJustBeforeFinished()
        throws Exception
    {
        final CountDownLatch latch = new CountDownLatch( 2 );
        final JoinableOutputStream stream = startTimedWrite( 1, latch );
        startRead( 18000, stream, latch );
        //        startRead( 500, stream, latch );

        System.out.println( "Waiting for " + name.getMethodName() + " threads to complete." );
        latch.await();
    }

    @Test
    public void joinFileWriteAndCloseBeforeFinished()
        throws Exception
    {
        final CountDownLatch latch = new CountDownLatch( 2 );
        final JoinableOutputStream stream = startTimedWrite( 1, latch );
        startRead( 0, -1, 10000, stream, latch );
        //        startRead( 500, stream, latch );

        System.out.println( "Waiting for " + name.getMethodName() + " threads to complete." );
        latch.await();
    }

    private void startRead( final long initialDelay, final JoinableOutputStream stream, final CountDownLatch latch )
    {
        startRead( initialDelay, -1, -1, stream, latch );
    }

    private void startRead( final long initialDelay, final long readDelay, final long closeDelay,
                            final JoinableOutputStream stream,
                            final CountDownLatch latch )
    {
        final Thread t = new Thread( new AsyncFileReader( initialDelay, readDelay, closeDelay, stream, latch ) );
        t.setName( name.getMethodName() + "/reader" + readers++ );
        t.setDaemon( true );
        t.start();
    }

    private void startTimedRawRead( final File file, final long initialDelay, final long readDelay,
                                    final long closeDelay,
                                    final CountDownLatch latch )
    {
        final Thread t = new Thread( new AsyncFileReader( initialDelay, readDelay, closeDelay, file, latch ) );
        t.setName( name.getMethodName() + "/reader" + readers++ );
        t.setDaemon( true );
        t.start();
    }

    private JoinableOutputStream startTimedWrite( final long delay, final CountDownLatch latch )
        throws Exception
    {
        final File file = temp.newFile();
        return startTimedWrite( file, delay, latch );
    }

    private JoinableOutputStream startTimedWrite( final File file, final long delay, final CountDownLatch latch )
        throws Exception
    {
        final JoinableOutputStream stream = new JoinableOutputStream( file );

        final Thread t = new Thread( new TimedFileWriter( stream, delay, latch ) );
        t.setName( name.getMethodName() + "/writer" );
        t.setDaemon( true );
        t.start();

        return stream;
    }

    public static final class AsyncFileReader
        implements Runnable
    {

        private final JoinableOutputStream stream;

        private final CountDownLatch latch;

        private final long initialDelay;

        private final long closeDelay;

        private final File file;

        private final long readDelay;

        public AsyncFileReader( final long initialDelay, final long readDelay, final long closeDelay,
                                final JoinableOutputStream stream,
                                final CountDownLatch latch )
        {
            this.initialDelay = initialDelay;
            this.readDelay = readDelay;
            this.closeDelay = closeDelay;
            this.stream = stream;
            this.latch = latch;
            this.file = null;
        }

        public AsyncFileReader( final long initialDelay, final long readDelay, final long closeDelay, final File file,
                                final CountDownLatch latch )
        {
            this.initialDelay = initialDelay;
            this.readDelay = readDelay;
            this.closeDelay = closeDelay;
            this.file = file;
            this.latch = latch;
            this.stream = null;
        }

        @Override
        public void run()
        {
            System.out.println( Thread.currentThread()
                                      .getName() + ": Waiting " + initialDelay + "ms to initialize read." );
            try
            {
                Thread.sleep( initialDelay );
            }
            catch ( final InterruptedException e )
            {
                return;
            }

            InputStream inStream = null;
            try
            {
                if ( stream != null )
                {
                    inStream = stream.joinStream();
                }
                else
                {
                    inStream = new FileInputStream( file );
                }
                System.out.println( Thread.currentThread()
                                          .getName() + ": Opened read stream" );

                if ( readDelay > -1 )
                {
                    System.out.println( Thread.currentThread()
                                              .getName() + ": Reading with " + readDelay + "ms between each byte." );

                    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    int read = -1;
                    while ( ( read = inStream.read() ) > -1 )
                    {
                        baos.write( read );
                        Thread.sleep( readDelay );
                    }

                    System.out.println( baos );
                }
                else if ( closeDelay > -1 )
                {
                    System.out.println( Thread.currentThread()
                                              .getName() + ": Reading with total delay of " + closeDelay + "ms." );
                    final long end = System.currentTimeMillis() + closeDelay;
                    while ( inStream.read() > -1 && System.currentTimeMillis() < end )
                    {
                        Thread.sleep( 1 );
                    }
                }
                else
                {
                    final List<String> lines = IOUtils.readLines( inStream );

                    System.out.println( Thread.currentThread()
                                              .getName() + ": " + lines );

                    assertThat( Thread.currentThread()
                                      .getName() + " wrong number of lines read! Expected: " + COUNT + ", got: "
                        + lines.size(), lines.size(), equalTo( COUNT ) );
                }

                System.out.println( Thread.currentThread()
                                          .getName() + ": Read done." );
            }
            catch ( final Exception e )
            {
                e.printStackTrace();
            }
            finally
            {
                IOUtils.closeQuietly( inStream );
                latch.countDown();
            }
        }

    }

    public static final class TimedFileWriter
        implements Runnable
    {
        private final long delay;

        private final JoinableOutputStream stream;

        private final CountDownLatch latch;

        public TimedFileWriter( final JoinableOutputStream stream, final long delay, final CountDownLatch latch )
        {
            this.stream = stream;
            this.delay = delay;
            this.latch = latch;
        }

        @Override
        public void run()
        {
            System.out.println( Thread.currentThread()
                                      .getName() + ": initializing write." );
            try
            {
                System.out.println( Thread.currentThread()
                                          .getName() + ": Writing with per-line delay of: " + delay );
                for ( int i = 0; i < COUNT; i++ )
                {
                    //                    System.out.println( "Sleeping: " + delay );
                    Thread.sleep( delay );

                    //                    System.out.println( "Writing: " + i );
                    stream.write( String.format( "%d\n", i )
                                        .getBytes() );
                }

                System.out.println( Thread.currentThread()
                                          .getName() + ": write done." );
            }
            catch ( final Exception e )
            {
                e.printStackTrace();
            }
            finally
            {
                IOUtils.closeQuietly( stream );
                latch.countDown();
            }
        }
    }
}
