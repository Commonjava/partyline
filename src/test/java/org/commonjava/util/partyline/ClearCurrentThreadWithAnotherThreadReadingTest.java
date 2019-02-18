/**
 * Copyright (C) 2015 Red Hat, Inc. (nos-devel@redhat.com)
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
import org.commonjava.util.partyline.spi.JoinableFile;
import org.jboss.byteman.contrib.bmunit.BMUnitConfig;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.commonjava.util.partyline.fixture.ThreadDumper.timeoutRule;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Created by jdcasey on 1/3/17.
 *
 * Test that checks resource management under the following conditions:
 * <ul>
 *     <li>Two threads open {@link InputStream}s to the same file concurrently and start reading</li>
 *     <li>Thread 1 finishes reading first, and calls {@link Partyline#cleanupCurrentThread()} before Thread 2 is done reading</li>
 * </ul>
 * <br/>
 * <b>EXPECTED RESULT:</b> Thread 2 should be able to complete its read operation and close the {@link JoinableFile}
 * when it completes. This process should be <b>unaffected</b> by Thread 1's
 * {@link Partyline#cleanupCurrentThread()} call.
 */
public class ClearCurrentThreadWithAnotherThreadReadingTest
{
    @ClassRule
    public static TestRule timeout = timeoutRule( 4, TimeUnit.SECONDS );

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    private Logger logger = LoggerFactory.getLogger( getClass() );

    private CountDownLatch completionLatch = new CountDownLatch( 2 );

    /**
     * Given:
     * <ul>
     *     <li>Two threads, T1 and T2</li>
     *     <li>One file containing some content, targeted for reading by both threads</li>
     * </ul>
     * <br/>
     * Sequence:
     * <br/>
     * <ol>
     *   <li>T1 opens input stream</li>
     *   <li>T1 reads from input stream</li>
     *   <li>One of the following in unspecified order:
     *      <ol type="A">
     *          <li>T1 waits for T2 to open input stream</li>
     *          <li>T2 opens input stream</li>
     *      </ol>
     *   </li>
     *   <li>T1 calls {@link Partyline#cleanupCurrentThread()}</li>
     *   <li>T2 reads from input stream</li>
     * </ol>
     * <br/>
     * Expected Result: Both threads successfully read correct content from the file.
     */
    @Test
    @BMUnitConfig( debug = true )
    public void run()
            throws Exception
    {
        final ExecutorService execs = Executors.newFixedThreadPool( 2 );

        CountDownLatch t1StartLatch = new CountDownLatch( 1 );
        CountDownLatch t2StartLatch = new CountDownLatch( 1 );
        CountDownLatch t1CleanupLatch = new CountDownLatch( 1 );

        final Partyline manager = new Partyline();

        final String content = "This is a test";
        final File file = temp.newFile();

        FileUtils.write( file, content );

        List<String> returning = new ArrayList<String>();

        execs.execute( ()->{
            Thread.currentThread().setName( "T1" );

            readFile( null, t1StartLatch, null, manager, file, returning );
            try
            {
                logger.info( "Waiting for T2 to get an input stream" );
                t2StartLatch.await();
            }
            catch ( InterruptedException e )
            {
                logger.warn( Thread.currentThread().getName()
                                     + " interrupted while waiting for second reader to open input stream." );
            }

            logger.info( "Cleaning up T1 resources" );
            manager.cleanupCurrentThread();

            logger.info( "Signaling T1 cleanup is complete." );
            t1CleanupLatch.countDown();
        });

        execs.execute( ()->{
            Thread.currentThread().setName( "T2" );

            readFile( t1StartLatch, t2StartLatch, t1CleanupLatch, manager, file, returning );
        });

        completionLatch.await();

        assertThat( "Both threads should return content!", returning.size(), equalTo( 2 ) );
        assertThat( returning.get( 0 ), equalTo( content ) );
        assertThat( returning.get( 1 ), equalTo( content ) );
    }

    private void readFile( CountDownLatch start, CountDownLatch preReadLatch, CountDownLatch cleanupLatch, Partyline manager, File file,
                           List<String> returning )
    {
        logger.info( "Starting file read...", Thread.currentThread().getName() );

        try
        {
            // if we have a start latch, wait for it to clear before proceeding. This helps guarantee thread order.
            if ( start != null )
            {
                logger.info( "Waiting for pre-start latch to clear." );
                start.await();
            }

            logger.info( "Opening input stream" );
            try (InputStream s = manager.openInputStream( file ))
            {
                // if we have a pre-read latch, count it down to signal that we have an input stream.
                if ( preReadLatch != null )
                {
                    logger.info( "Clearing pre-read latch" );
                    preReadLatch.countDown();
                }

                // if we have a cleanup latch, wait for it to ensure cleanupCurrentThread is called from the other thread before we try to read.
                if ( cleanupLatch != null )
                {
                    logger.info( "Waiting for other thread's cleanup latch to clear" );
                    cleanupLatch.await();
                }

                logger.info( "Reading file contents" );

                // read the file and store it to the returning list...
                returning.add( IOUtils.toString( s ) );
            }
            catch ( Exception e )
            {
                e.printStackTrace();
                fail( "Failed to open inputStream: " + e.getMessage() );
            }
        }
        catch ( InterruptedException e )
        {
            logger.warn( Thread.currentThread().getName() + " interrupted during open-read operation." );
        }
        finally
        {
            completionLatch.countDown();
            logger.info( "File read done" );
        }
    }
}
