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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.commonjava.util.partyline.fixture.AbstractJointedIOTest;
import org.commonjava.util.partyline.fixture.TimedTask;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JoinableFileTest
    extends AbstractJointedIOTest
{

    @Test
    public void lockDirectory()
            throws IOException
    {
        File dir = temp.newFolder();
        dir.mkdirs();
        JoinableFile jf = new JoinableFile( dir, new LockOwner(), false );

        assertThat( jf.isWriteLocked(), equalTo( true ) );

        jf.close();
    }

    @Test
    public void lockDirectoryCannotBeWritten()
            throws IOException
    {
        File dir = temp.newFolder();
        dir.mkdirs();
        JoinableFile jf = new JoinableFile( dir, new LockOwner(), false );

        assertThat( jf.isWriteLocked(), equalTo( true ) );

        OutputStream out = jf.getOutputStream();
        assertThat( out, nullValue() );

        jf.close();
    }

    @Test
    public void lockDirectoryIsNotJoinable()
            throws IOException
    {
        File dir = temp.newFolder();
        dir.mkdirs();
        JoinableFile jf = new JoinableFile( dir, new LockOwner(), false );

        assertThat( jf.isWriteLocked(), equalTo( true ) );
        assertThat( jf.isJoinable(), equalTo( false ) );

        jf.close();
    }

    @Test( expected = IOException.class )
    public void lockDirectoryJoinFails()
            throws IOException
    {
        File dir = temp.newFolder();
        dir.mkdirs();
        JoinableFile jf = new JoinableFile( dir, new LockOwner(), false );

        assertThat( jf.isWriteLocked(), equalTo( true ) );

        jf.joinStream();
    }

    @Test
    public void writeToFile()
        throws Exception
    {
        final CountDownLatch latch = new CountDownLatch( 1 );
        final JoinableFile stream = startTimedWrite( 0, latch );

        System.out.println( "Waiting for " + name.getMethodName() + " threads to complete." );
        latch.await();

        final File file = new File( stream.getPath() );

        System.out.println( "File length: " + file.length() );

        final List<String> lines = FileUtils.readLines( file );
        System.out.println( lines );

        assertThat( lines.size(), equalTo( COUNT ) );
    }

    @Test
    public void overwriteFile_SmallerReplacementTruncates()
            throws Exception
    {
        File f = temp.newFile();
        JoinableFile jf = new JoinableFile( f, new LockOwner(), true );
        OutputStream stream = jf.getOutputStream();

        String longer = "This is a really really really long string";
        stream.write( longer.getBytes() );
        stream.close();

        jf = new JoinableFile( f,new LockOwner(), true );
        stream = jf.getOutputStream();

        String shorter = "This is a short string";
        stream.write( shorter.getBytes() );
        stream.close();

        final File file = new File( jf.getPath() );

        System.out.println( "File length: " + file.length() );
        assertThat( file.length(), equalTo( (long) shorter.getBytes().length ) );

        String content = FileUtils.readFileToString( f );
        assertThat( content, equalTo( shorter ) );
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
        final JoinableFile stream = startTimedWrite( 0, latch );
        startRead( 0, stream, latch );

        System.out.println( "Waiting for " + name.getMethodName() + " threads to complete." );
        latch.await();
    }

    @Test
    public void joinFileWriteTwiceWithDelay()
        throws Exception
    {
        final CountDownLatch latch = new CountDownLatch( 3 );
        final JoinableFile stream = startTimedWrite( 1, latch );
        startRead( 0, stream, latch );
        startRead( 500, stream, latch );

        System.out.println( "Waiting for " + name.getMethodName() + " threads to complete." );
        latch.await();
    }

    @Test
    public void joinFileRead()
            throws Exception
    {
        final File file = temp.newFile();
        final File f = temp.newFile();
        String str = "This is a test";
        FileUtils.write( f, str );
        JoinableFile jf = null;
        InputStream s1 = null;
        InputStream s2 = null;

        Logger logger = LoggerFactory.getLogger( getClass() );
        try
        {
            jf = new JoinableFile( f, new LockOwner(), false );
            s1 = jf.joinStream();
            s2 = jf.joinStream();

            logger.info( "READ first thread" );
            String out1 = IOUtils.toString( s1 );
            logger.info( "READ second thread" );
            String out2 = IOUtils.toString( s2 );

            assertThat( "first reader returned wrong data", out1, equalTo( str ) );
            assertThat( "second reader returned wrong data", out2, equalTo( str ) );
        }
        finally
        {
            logger.info( "CLOSE first thread" );
            IOUtils.closeQuietly( s1 );
            logger.info( "CLOSE second thread" );
            IOUtils.closeQuietly( s2 );
        }

        assertThat( jf, notNullValue() );
        assertThat( jf.isOpen(), equalTo( false ) );
    }

    @Test
    public void joinFileWriteContinueAfterInputStreamClose()
            throws Exception
    {
        final CountDownLatch latch = new CountDownLatch( 1 );
        final JoinableFile stream = startTimedWrite( 1, latch );

        InputStream inStream = stream.joinStream();
        InputStream inStream2 = stream.joinStream();
        Thread.sleep(1000);
        inStream.close();
        inStream2.close();
        System.out.println( "All input stream closed. Waiting for " + name.getMethodName() + " writer thread to complete." );
        latch.await();

        final File file = new File( stream.getPath() );
        System.out.println( "File length: " + file.length() );

        final List<String> lines = FileUtils.readLines( file );
        System.out.println( lines );

        assertThat( lines.size(), equalTo( COUNT ) );
    }

    @Test
    public void joinFileWriteJustBeforeFinished()
        throws Exception
    {
        final CountDownLatch latch = new CountDownLatch( 2 );
        final JoinableFile stream = startTimedWrite( 1, latch );
        startRead( 1000, stream, latch );
        //        startRead( 500, stream, latch );

        System.out.println( "Waiting for " + name.getMethodName() + " threads to complete." );
        latch.await();
    }

    @Test
    public void joinFileWriteAndCloseBeforeFinished()
        throws Exception
    {
        final CountDownLatch latch = new CountDownLatch( 2 );
        final JoinableFile stream = startTimedWrite( 1, latch );
        startRead( 0, -1, 10000, stream, latch );
        //        startRead( 500, stream, latch );

        System.out.println( "Waiting for " + name.getMethodName() + " threads to complete." );
        latch.await();
    }

    @Test
    @Ignore( "Using reference counts to close JoinableOutputStream when last input closes." )
    public void outputStreamWaitsForSingleJoinedInputStreamToClose()
        throws Exception
    {
        final JoinableFile jf = new JoinableFile( temp.newFile(), new LockOwner(), true );

        final String out = "output";
        final String in = "input";
        final Map<String, Long> timings =
            testTimings( new TimedTask( out, new WaitThenCloseOutputStream( 5, jf ) ),
                        new TimedTask( in, new OpenThenWaitThenCloseInputStream( 10, jf ) ) );

        System.out.println( "input closed at: " + timings.get( in ) );
        System.out.println( "output closed at: " + timings.get( out ) );

        assertThat( "input jf (" + timings.get( in ) + ") should have closed before output jf ("
                        + timings.get( out ) + ")", timings.get( in ) <= timings.get( out ), equalTo( true ) );
    }

    @Test
    @Ignore( "Using reference counts to close JoinableOutputStream when last input closes." )
    public void outputStreamWaitsForTwoJoinedInputStreamsToClose()
        throws Exception
    {
        final JoinableFile jf = new JoinableFile( temp.newFile(), new LockOwner(), true );

        final String out = "output";
        final String in = "input";
        final String in2 = "input2";
        final Map<String, Long> timings =
            testTimings( new TimedTask( out, new WaitThenCloseOutputStream( 5, jf ) ),
                        new TimedTask( in, new OpenThenWaitThenCloseInputStream( 10, jf ) ),
                        new TimedTask( in2, new OpenThenWaitThenCloseInputStream( 15, jf ) ) );

        System.out.println( "input 1 closed at: " + timings.get( in ) );
        System.out.println( "input 2 closed at: " + timings.get( in2 ) );
        System.out.println( "output closed at: " + timings.get( out ) );

        assertThat( timings.get( in ) <= timings.get( out ), equalTo( true ) );
        assertThat( timings.get( in ) <= timings.get( in2 ), equalTo( true ) );
        assertThat( timings.get( in2 ) <= timings.get( out ), equalTo( true ) );
    }

    public static final class WaitThenCloseOutputStream
        implements Runnable
    {
        private final JoinableFile jf;

        private final long timeout;

        public WaitThenCloseOutputStream( final long timeout, final JoinableFile jf )
        {
            this.timeout = timeout;
            this.jf = jf;
        }

        @Override
        public void run()
        {
            try
            {
                Thread.sleep( timeout );
                jf.close();
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
        private final JoinableFile jf;

        private final long timeout;

        public OpenThenWaitThenCloseInputStream( final long timeout, final JoinableFile jf )
        {
            this.timeout = timeout;
            this.jf = jf;
        }

        @Override
        public void run()
        {
            try
            {
                final InputStream in = jf.joinStream();
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
