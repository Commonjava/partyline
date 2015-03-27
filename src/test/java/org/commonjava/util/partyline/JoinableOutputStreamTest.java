package org.commonjava.util.partyline;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.commonjava.util.partyline.JoinableOutputStream;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

public class JoinableOutputStreamTest
{
    private static final int COUNT = 20000;

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
        startRead( 0, 10000, stream, latch );
        //        startRead( 500, stream, latch );

        System.out.println( "Waiting for " + name.getMethodName() + " threads to complete." );
        latch.await();
    }

    private void startRead( final long initialDelay, final JoinableOutputStream stream, final CountDownLatch latch )
    {
        startRead( initialDelay, -1, stream, latch );
    }

    private void startRead( final long initialDelay, final long closeDelay, final JoinableOutputStream stream,
                            final CountDownLatch latch )
    {
        final Thread t = new Thread( new AsyncFileReader( initialDelay, closeDelay, stream, latch ) );
        t.setName( name.getMethodName() + "/reader" + readers++ );
        t.setDaemon( true );
        t.start();
    }

    private JoinableOutputStream startTimedWrite( final long delay, final CountDownLatch latch )
        throws Exception
    {
        final File file = temp.newFile();
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

        public AsyncFileReader( final long initialDelay, final long closeDelay, final JoinableOutputStream stream,
                                final CountDownLatch latch )
        {
            this.initialDelay = initialDelay;
            this.closeDelay = closeDelay;
            this.stream = stream;
            this.latch = latch;
        }

        @Override
        public void run()
        {
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
                inStream = stream.joinStream();
                if ( closeDelay > -1 )
                {
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
            try
            {
                for ( int i = 0; i < COUNT; i++ )
                {
                    //                    System.out.println( "Sleeping: " + delay );
                    Thread.sleep( delay );

                    //                    System.out.println( "Writing: " + i );
                    stream.write( String.format( "%d\n", i )
                                        .getBytes() );
                }
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
