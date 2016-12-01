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

import org.apache.commons.io.IOUtils;
import org.commonjava.util.partyline.fixture.TimedFileWriter;
import org.commonjava.util.partyline.fixture.TimedTask;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.rules.TestRule;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.commonjava.util.partyline.fixture.ThreadDumper.timeoutRule;
import static org.junit.Assert.fail;

public abstract class AbstractJointedIOTest
{

    public static final int COUNT = 2000;

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    @Rule
    public TestName name = new TestName();

    protected int readers = 0;

    protected int writers = 0;

    protected int timers = 0;

    protected Thread newThread( final String named, final Runnable runnable )
    {
        final Thread t = new Thread( runnable );
        t.setName( named );
        t.setDaemon( true );
        t.setUncaughtExceptionHandler( new UncaughtExceptionHandler()
        {
            @Override
            public void uncaughtException( final Thread t, final Throwable e )
            {
                e.printStackTrace();
                Assert.fail( t.getName() + ": " + e.getMessage() );
            }
        } );
        return t;
    }

    protected Map<String, Long> testTimings( final long startDelay, final TimedTask... tasks )
    {
        return testTimings( startDelay, Arrays.asList( tasks ) );
    }

    protected Map<String, Long> testTimings( final TimedTask... tasks )
    {
        return testTimings( 2, Arrays.asList( tasks ) );
    }

    protected Map<String, Long> testTimings( final List<TimedTask> tasks )
    {
        return testTimings( 2, tasks );
    }

    protected Map<String, Long> testTimings( final long startDelay, final List<TimedTask> tasks )
    {
        final CountDownLatch latch = new CountDownLatch( tasks.size() );
        for ( final TimedTask task : tasks )
        {
            task.setLatch( latch );
            newThread( task.getName(), task ).start();
            try
            {
                Thread.sleep( startDelay );
            }
            catch ( final InterruptedException e )
            {
                Assert.fail( "Interrupted!" );
            }
        }

        try
        {
            latch.await();
        }
        catch ( final InterruptedException e )
        {
            Assert.fail( "Interrupted!" );
        }

        final Map<String, Long> timings = new HashMap<>();
        for ( final TimedTask task : tasks )
        {
            timings.put( task.getName(), task.getTimestamp() );
        }

        return timings;
    }

    protected void startRead( final long initialDelay, final JoinableFile stream, final CountDownLatch latch )
    {
        startRead( initialDelay, -1, -1, stream, latch );
    }

    protected void startRead( final long initialDelay, final long readDelay, final long closeDelay,
                              final JoinableFile stream, final CountDownLatch latch )
    {
        newThread( "reader" + readers++,
                   new AsyncFileReader( initialDelay, readDelay, closeDelay, stream, latch ) ).start();
    }

    protected void startTimedRawRead( final File file, final long initialDelay, final long readDelay,
                                      final long closeDelay, final CountDownLatch latch )
    {
        newThread( "reader" + readers++,
                   new AsyncFileReader( initialDelay, readDelay, closeDelay, file, latch ) ).start();
    }

    protected JoinableFile startTimedWrite( final long delay, final CountDownLatch latch )
            throws Exception
    {
        final File file = temp.newFile();
        return startTimedWrite( file, delay, latch );
    }

    protected JoinableFile startTimedWrite( final File file, final long delay, final CountDownLatch latch )
            throws Exception
    {
        String threadName = "writer" + writers++;
        final JoinableFile jf =
                new JoinableFile( file, new LockOwner( threadName, name.getMethodName(), LockLevel.write ), true );

        newThread( threadName, new TimedFileWriter( jf, delay, latch ) ).start();

        return jf;
    }

    protected void latchWait( CountDownLatch latch )
    {
        try
        {
            latch.await();
        }
        catch ( InterruptedException e )
        {
            System.out.println( "Threads await Exception." );
        }
    }

    abstract static class IOTask
            implements Runnable
    {
        protected JoinableFileManager fileManager;

        protected String content;

        protected CountDownLatch controlLatch;

        protected long waiting;

        protected File file;

        protected IOTask( JoinableFileManager fileManager, String content, File file, CountDownLatch controlLatch,
                          long waiting )
        {
            this.fileManager = fileManager;
            this.content = content;
            this.file = file;
            this.controlLatch = controlLatch;
            this.waiting = waiting;
        }
    }

    static final class ReadTask
            extends IOTask
            implements Callable<String>
    {
        private String readingResult;

        public ReadTask( JoinableFileManager fileManager, String content, File file, CountDownLatch controlLatch )
        {
            super( fileManager, content, file, controlLatch, -1 );
        }

        public ReadTask( JoinableFileManager fileManager, String content, File file, CountDownLatch controlLatch,
                         long waiting )
        {
            super( fileManager, content, file, controlLatch, waiting );
        }

        @Override
        public void run()
        {
            try
            {
                final InputStream in = fileManager.openInputStream( file );
                if ( in == null )
                {
                    System.out.println( "Can not read content as the input stream is null." );
                    controlLatch.countDown();
                    return;
                }
                final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                int read = -1;
                final byte[] buf = new byte[512];
                System.out.println(
                        String.format( "<<<ReadTask>>> will start to read from the resource with inputStream %s",
                                       in.getClass().getName() ) );
                while ( ( read = in.read( buf ) ) > -1 )
                {
                    if ( waiting > 0 )
                    {
                        Thread.sleep( waiting );
                    }
                    baos.write( buf, 0, read );
                }
                baos.close();
                in.close();
                System.out.println( String.format( "<<<ReadTask>>> reading from the resource done with inputStream %s",
                                                   in.getClass().getName() ) );
                readingResult = new String( baos.toByteArray(), "UTF-8" );
                controlLatch.countDown();
            }
            catch ( Exception e )
            {
                System.out.println( "Read Task Runtime Exception." );
                e.printStackTrace();
            }
        }

        @Override
        public String call()
        {
            this.run();
            return readingResult;
        }
    }

    static final class OpenOutputStreamTask
            extends IOTask
            implements Callable<String>
    {
        private OutputStream stream;

        protected OpenOutputStreamTask( JoinableFileManager fileManager, String content, File file )
        {
            super( fileManager, content, file, null, -1 );
        }

        protected OpenOutputStreamTask( JoinableFileManager fileManager, String content, File file, long waiting )
        {
            super( fileManager, content, file, null, waiting );
        }

        protected OpenOutputStreamTask( JoinableFileManager fileManager, String content, File file,
                                        CountDownLatch controlLatch )
        {
            super( fileManager, content, file, controlLatch, -1 );
        }

        protected OpenOutputStreamTask( JoinableFileManager fileManager, String content, File file,
                                        CountDownLatch controlLatch, long waiting )
        {
            super( fileManager, content, file, controlLatch, waiting );
        }

        @Override
        public void run()
        {
            try
            {
                stream = fileManager.openOutputStream( file );
            }
            catch ( final Exception e )
            {
                e.printStackTrace();
                fail( "Failed to open stream: " + e.getMessage() );
            }

            if ( stream != null && waiting > -1 )
            {
                if ( waiting > 0 )
                {
                    try
                    {
                        Thread.sleep( waiting );
                    }
                    catch ( final InterruptedException e )
                    {
                        fail( "Interrupted!" );
                    }
                }
                IOUtils.closeQuietly( stream );
            }
            if ( controlLatch != null )
            {
                controlLatch.countDown();
            }
        }

        @Override
        public String call()
                throws Exception
        {
            this.run();
            return String.valueOf( System.nanoTime() );
        }
    }

    static final class OpenInputStreamTask
            extends IOTask
            implements Callable<String>
    {

        private InputStream stream;

        private String calling;

        protected OpenInputStreamTask( JoinableFileManager fileManager, String content, File file,
                                       CountDownLatch controlLatch )
        {
            super( fileManager, content, file, controlLatch, -1 );
        }

        protected OpenInputStreamTask( JoinableFileManager fileManager, String content, File file,
                                       CountDownLatch controlLatch, long waiting )
        {
            super( fileManager, content, file, controlLatch, waiting );
        }

        @Override
        public void run()
        {
            try
            {
                stream = fileManager.openInputStream( file );
                if ( stream == null )
                {
                    System.out.println( "Can not read content as the input stream is null." );
                    controlLatch.countDown();
                    return;
                }
                calling = IOUtils.toString( stream );
            }
            catch ( final Exception e )
            {
                e.printStackTrace();
                fail( "Failed to open stream: " + e.getMessage() );
            }

            if ( waiting > 0 )
            {
                try
                {
                    Thread.sleep( waiting );
                }
                catch ( final InterruptedException e )
                {
                    fail( "Interrupted!" );
                }
            }
            IOUtils.closeQuietly( stream );
            controlLatch.countDown();
        }

        @Override
        public String call()
                throws Exception
        {
            this.run();
            return calling;
        }
    }

    static final class OpenInputStreamWithTimeoutTask
            extends IOTask
            implements Callable<String>
    {

        private InputStream stream;

        private String calling;

        private long timingout;

        protected OpenInputStreamWithTimeoutTask( JoinableFileManager fileManager, String content, File file,
                                                  CountDownLatch controlLatch, long timingout )
        {
            super( fileManager, content, file, controlLatch, -1 );
            this.timingout = timingout;
        }

        @Override
        public void run()
        {
            try
            {
                stream = fileManager.openInputStream( file, timingout );
                if ( stream == null )
                {
                    System.out.println( "Can not read content as the input stream is null." );
                    controlLatch.countDown();
                    return;
                }
                calling = IOUtils.toString( stream );
            }
            catch ( final Exception e )
            {
                e.printStackTrace();
                fail( "Failed to open stream: " + e.getMessage() );
            }

            IOUtils.closeQuietly( stream );
            controlLatch.countDown();
        }

        @Override
        public String call()
                throws Exception
        {
            this.run();
            return calling;
        }
    }
}
