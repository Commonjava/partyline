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

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.commonjava.util.partyline.fixture.AbstractJointedIOTest;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMScript;
import org.jboss.byteman.contrib.bmunit.BMUnitConfig;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.commonjava.util.partyline.fixture.ThreadDumper.dumpThreads;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith( org.jboss.byteman.contrib.bmunit.BMUnitRunner.class )
@BMUnitConfig( loadDirectory = "target/test-classes/bmunit", debug = true )
public class JoinableFileManagerConcurrentTest
        extends AbstractJointedIOTest
{
    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    private final ExecutorService testPool = Executors.newFixedThreadPool( 2 );

    private final CountDownLatch latch = new CountDownLatch( 2 );

    private final JoinableFileManager fileManager = new JoinableFileManager();

    private void latchWait( CountDownLatch latch )
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

    private void writeFile( File file, String content )
            throws IOException
    {
        final OutputStream out = new FileOutputStream( file );
        IOUtils.write( content, out );
        out.close();
    }

    @Test
    @Ignore
    @BMScript( "DeleteWhileInputStreamOpen.btm" )
    @BMUnitConfig( enforce = true, verbose = true )
    public void deleteWaitsForOpenInputStreamToClose()
            throws Exception
    {
        String src = "This is a test";

        File f = temp.newFile( "test.txt" );
        FileUtils.write( f, src );

        Future<Boolean> streamFuture = testPool.submit( () -> {
            Thread.currentThread().setName( name.getMethodName() + "::openInputStream" );
            InputStream stream = null;
            try
            {
                stream = fileManager.openInputStream( f );
                System.out.println( stream + " doing stuff..." );
                stream.close();
            }
            catch ( final IOException e )
            {
                e.printStackTrace();
                fail( "Failed to open stream: " + e.getMessage() );
            }
            finally
            {
                latch.countDown();
            }
            return true;
        } );

        Future<Boolean> deleteFuture = testPool.submit( () -> {
            Thread.currentThread().setName( name.getMethodName() + "::tryDelete" );
            try
            {
                return fileManager.tryDelete( f );
            }
            finally
            {
                latch.countDown();
            }
        } );

        latchWait( latch );

        boolean result = deleteFuture.get();
        assertThat( "File still exists", f.exists(), equalTo( false ) );
        assertTrue( "File was not deleted!", result );
    }

    @Test
    @BMScript( "TryToBothRead.btm" )
    public void testTryToBothRead()
            throws Exception
    {
        final String content = "This is a bmunit test";
        final File file = temp.newFile( "file_both_read.txt" );
        writeFile( file, content );
        final Future<String> readingFuture1 =
                testPool.submit( (Callable<String>) new ReadTask( fileManager, content, file, latch ) );
        final Future<String> readingFuture2 =
                testPool.submit( (Callable<String>) new ReadTask( fileManager, content, file, latch ) );

        latchWait( latch );

        final String readingResult1 = readingFuture1.get();
        assertThat( readingResult1, equalTo( content ) );
        final String readingResult2 = readingFuture2.get();
        assertThat( readingResult2, equalTo( content ) );
    }

    /**
     * Test that locks for deletion and then checks that locks associated with mutiple read FAILURES clear correctly.
     * This will setup an intricate script of events for a single file, where:
     * <ol>
     *     <li>Deletion happens first</li>
     *     <li>Multiple reads happen simultaneously, and fail because the file doesn't exist</li>
     *     <li>A single write at the end ensures the other locks are clear</li>
     * </ol>
     * @throws Exception
     */
    @BMRules( rules = {
            // setup the rendezvous for all threads, which will mean everything else waits for delete to exit.
            @BMRule( name = "init rendezvous", targetClass = "JoinableFileManager",
                                 targetMethod = "<init>",
                                 targetLocation = "ENTRY",
                                 action = "createRendezvous(\"deleted\", 5)" ),

            // setup the pause for openOutputStream, which rendezvous with openInputStream AND tryDelete, then
            // waits for one openInputStream call to exit
            @BMRule( name = "openOutputStream start", targetClass = "JoinableFileManager",
                     targetMethod = "openOutputStream",
                     targetLocation = "ENTRY",
                     action = "debug(\"Waiting for READ to start.\"); rendezvous(\"deleted\"); waitFor(\"reading\"); debug(Thread.currentThread().getName() + \": openInputStream() thread proceeding.\")" ),

            // setup the rendezvous to wait for tryDelete to exit
            @BMRule( name = "openInputStream start", targetClass = "JoinableFileManager",
                     targetMethod = "openInputStream",
                     targetLocation = "ENTRY",
                     action = "debug(\"Waiting for DELETE to finish.\"); rendezvous(\"deleted\"); debug(Thread.currentThread().getName() + \": openInputStream() thread proceeding.\")" ),

            // setup the trigger to signal openOutputStream when the first openInputStream exits
            @BMRule( name = "openInputStream end", targetClass = "JoinableFileManager",
                     targetMethod = "openInputStream",
                     targetLocation = "EXCEPTION EXIT",
                     condition = "waiting(\"reading\")",
                     action = "debug(\"Signal READ.\"); signalWake(\"reading\"); debug(Thread.currentThread().getName() + \": openInputStream() done.\")" ),

            // setup the trigger to signal all other threads to resume once tryDelete exits
            @BMRule( name = "tryDelete end", targetClass = "JoinableFileManager",
                     targetMethod = "tryDelete",
                     targetLocation = "EXIT",
                     action = "debug(\"Signal DELETE.\"); rendezvous(\"deleted\"); debug(Thread.currentThread().getName() + \": delete() done.\")" ) } )
    @Test
    public void testDeleteLockReleaseAndConcurrentReadFailureLockAvoidance()
            throws Exception
    {
        final ExecutorService execs = Executors.newFixedThreadPool( 5 );
        final File f = temp.newFile( "child.txt" );
//        FileUtils.write( f, "test data" );

        final CountDownLatch latch = new CountDownLatch( 5 );
        final JoinableFileManager manager = new JoinableFileManager();
        final long start = System.currentTimeMillis();

        execs.execute( () -> {
            Thread.currentThread().setName( "openOutputStream" );

            try (OutputStream o = manager.openOutputStream( f ))
            {
                o.write( "Test data".getBytes() );
            }
            catch ( Exception e )
            {
                e.printStackTrace();
            }
            latch.countDown();
            System.out.println(
                    String.format( "[%s] Count down after write thread: %s", Thread.currentThread().getName(),
                                   latch.getCount() ) );
        } );

        for ( int i = 0; i < 3; i++ )
        {
            final int k = i;
            execs.execute( () -> {
                Thread.currentThread().setName( "openInputStream-" + k );
                try (InputStream s = manager.openInputStream( f ))
                {
                    System.out.println( s );
                }
                catch ( Exception e )
                {
                    e.printStackTrace();
                }
                finally
                {
                    latch.countDown();
                    System.out.println(
                            String.format( "[%s] Count down after %s read thread: %s", Thread.currentThread().getName(),
                                           k, latch.getCount() ) );
                }
            } );
        }

        execs.execute( () -> {
            Thread.currentThread().setName( "delete" );
            try
            {
                manager.tryDelete( f );
            }
            catch ( Exception e )
            {
                e.printStackTrace();
            }
            finally
            {
                latch.countDown();
                System.out.println(
                        String.format( "[%s] Count down after delete thread: %s", Thread.currentThread().getName(),
                                       latch.getCount() ) );
            }
        } );

        latch.await( 6, TimeUnit.SECONDS );
        final long end = System.currentTimeMillis();
        final long waste = end - start;
        if ( waste >= 6000 )
        {
            String error = "Waited beyond 6 second timeout for operations";
            System.out.println( error );
            dumpThreads();
            fail( error );
        }
        assertTrue( "Waited beyond timeout for operations to complete.", waste < 6000 );
    }

    /**
     * Test that locks for mutiple reads clear correctly. This will setup an script of events for
     * a single file, where:
     * <ol>
     *     <li>Multiple reads happen simultaneously, read the content, and close</li>
     *     <li>A single write at the end ensures the other locks are clear</li>
     * </ol>
     * @throws Exception
     */
    @BMRules( rules = {
            // setup the rendezvous for all threads, which will mean everything waits until all threads are started.
            @BMRule( name = "init rendezvous", targetClass = "JoinableFileManager",
                     targetMethod = "<init>",
                     targetLocation = "ENTRY",
                     action = "createRendezvous(\"begin\", 4)" ),

            // setup the pause for openOutputStream, which rendezvous with openInputStream calls, then
            // waits for one openInputStream call to exit
            @BMRule( name = "openOutputStream start", targetClass = "JoinableFileManager",
                     targetMethod = "openOutputStream",
                     targetLocation = "ENTRY",
                     action = "debug(\"Waiting for READ to start.\"); rendezvous(\"begin\"); waitFor(\"reading\"); debug(Thread.currentThread().getName() + \": openInputStream() thread proceeding.\")" ),

            // setup the rendezvous to wait for all threads to be ready before proceeding
            @BMRule( name = "openInputStream start", targetClass = "JoinableFileManager",
                     targetMethod = "openInputStream",
                     targetLocation = "ENTRY",
                     action = "debug(\"Waiting for DELETE to finish.\"); rendezvous(\"begin\"); debug(Thread.currentThread().getName() + \": openInputStream() thread proceeding.\")" ),

            // setup the trigger to signal openOutputStream when the first openInputStream exits
            @BMRule( name = "openInputStream end", targetClass = "JoinableFileManager",
                     targetMethod = "openInputStream",
                     targetLocation = "EXIT",
                     condition = "waiting(\"reading\")",
                     action = "debug(\"Signal READ.\"); signalWake(\"reading\"); debug(Thread.currentThread().getName() + \": openInputStream() done.\")" ) } )
    @Test
    public void testConcurrentReadsClearLocksAppropriately()
            throws Exception
    {
        final ExecutorService execs = Executors.newFixedThreadPool( 5 );
        final File f = temp.newFile( "child.txt" );
        FileUtils.write( f, "test data" );

        final CountDownLatch latch = new CountDownLatch( 4 );
        final JoinableFileManager manager = new JoinableFileManager();
        final long start = System.currentTimeMillis();

        execs.execute( () -> {
            Thread.currentThread().setName( "openOutputStream" );

            try (OutputStream o = manager.openOutputStream( f ))
            {
                o.write( "Test data".getBytes() );
            }
            catch ( Exception e )
            {
                e.printStackTrace();
            }
            latch.countDown();
            System.out.println(
                    String.format( "[%s] Count down after write thread: %s", Thread.currentThread().getName(),
                                   latch.getCount() ) );
        } );

        for ( int i = 0; i < 3; i++ )
        {
            final int k = i;
            execs.execute( () -> {
                Thread.currentThread().setName( "openInputStream-" + k );
                try (InputStream s = manager.openInputStream( f ))
                {
                    System.out.println( IOUtils.toString( s ) );
                }
                catch ( Exception e )
                {
                    e.printStackTrace();
                }
                finally
                {
                    latch.countDown();
                    System.out.println(
                            String.format( "[%s] Count down after %s read thread: %s", Thread.currentThread().getName(),
                                           k, latch.getCount() ) );
                }
            } );
        }

        latch.await( 6, TimeUnit.SECONDS );
        final long end = System.currentTimeMillis();
        final long waste = end - start;
        if ( waste >= 6000 )
        {
            String error = "Waited beyond 6 second timeout for operations";
            System.out.println( error );
            dumpThreads();
            fail( error );
        }
        assertTrue( "Waited beyond timeout for operations to complete.", waste < 6000 );
    }

    private abstract class IOTask
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

    private final class ReadTask
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
}
