/*******************************************************************************
* Copyright (c) 2015 ${owner}
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the GNU Public License v3.0
* which accompanies this distribution, and is available at
* http://www.gnu.org/licenses/gpl.html
*
* Contributors:
* ${owner} - initial API and implementation
******************************************************************************/
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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.io.FileUtils;
import org.commonjava.util.partyline.fixture.AbstractJointedIOTest;
import org.commonjava.util.partyline.fixture.TimedTask;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class JoinableOutputStreamTest
    extends AbstractJointedIOTest
{
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
    @Ignore( "This should NOT work...and the file manager should prevent it" )
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

        startTimedWrite( file, 1, latch );

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

    @Test
    public void outputStreamWaitsForSingleJoinedInputStreamToClose()
        throws Exception
    {
        final JoinableOutputStream stream = new JoinableOutputStream( temp.newFile() );

        final String out = "output";
        final String in = "input";
        final Map<String, Long> timings =
            testTimings( new TimedTask( out, new WaitThenCloseOutputStream( 5, stream ) ),
                        new TimedTask( in, new OpenThenWaitThenCloseInputStream( 10, stream ) ) );

        System.out.println( "input closed at: " + timings.get( in ) );
        System.out.println( "output closed at: " + timings.get( out ) );

        assertThat( "input stream (" + timings.get( in ) + ") should have closed before output stream ("
                        + timings.get( out ) + ")", timings.get( in ) < timings.get( out ), equalTo( true ) );
    }

    @Test
    public void outputStreamWaitsForTwoJoinedInputStreamsToClose()
        throws Exception
    {
        final JoinableOutputStream stream = new JoinableOutputStream( temp.newFile() );

        final String out = "output";
        final String in = "input";
        final String in2 = "input2";
        final Map<String, Long> timings =
            testTimings( new TimedTask( out, new WaitThenCloseOutputStream( 5, stream ) ),
                        new TimedTask( in, new OpenThenWaitThenCloseInputStream( 10, stream ) ),
                        new TimedTask( in2, new OpenThenWaitThenCloseInputStream( 15, stream ) ) );

        System.out.println( "input 1 closed at: " + timings.get( in ) );
        System.out.println( "input 2 closed at: " + timings.get( in2 ) );
        System.out.println( "output closed at: " + timings.get( out ) );

        assertThat( timings.get( in ) < timings.get( out ), equalTo( true ) );
        assertThat( timings.get( in ) < timings.get( in2 ), equalTo( true ) );
        assertThat( timings.get( in2 ) < timings.get( out ), equalTo( true ) );
    }

    public static final class WaitThenCloseOutputStream
        implements Runnable
    {
        private final JoinableOutputStream stream;

        private final long timeout;

        public WaitThenCloseOutputStream( final long timeout, final JoinableOutputStream stream )
        {
            this.timeout = timeout;
            this.stream = stream;
        }

        @Override
        public void run()
        {
            try
            {
                Thread.sleep( timeout );
                stream.close();
            }
            catch ( InterruptedException | IOException e )
            {
                e.printStackTrace();
                Assert.fail( "output stream did not close normally: " + e.getMessage() );
            }
        }
    }

    public static final class OpenThenWaitThenCloseInputStream
        implements Runnable
    {
        private final JoinableOutputStream stream;

        private final long timeout;

        public OpenThenWaitThenCloseInputStream( final long timeout, final JoinableOutputStream stream )
        {
            this.timeout = timeout;
            this.stream = stream;
        }

        @Override
        public void run()
        {
            try
            {
                final InputStream in = stream.joinStream();
                Thread.sleep( timeout );
                in.close();
            }
            catch ( InterruptedException | IOException e )
            {
                e.printStackTrace();
                Assert.fail( "output stream did not close normally: " + e.getMessage() );
            }
        }
    }
}
