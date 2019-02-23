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
import org.commonjava.cdi.util.weft.PoolWeftExecutorService;
import org.commonjava.cdi.util.weft.ThreadContext;
import org.commonjava.util.partyline.fixture.ThreadDumper;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Created by jdcasey on 11/14/17.
 */
public class ManyReadersWithPreExistingWriterTest
        extends AbstractJointedIOTest
{
    private static final int THREADS = 30;

    private static final long WAIT = 0;

    private static final int ITERATIONS = 2;

    private ExecutorService executor = new PoolWeftExecutorService( "test",
                                                                    (ThreadPoolExecutor) Executors.newCachedThreadPool() );

    private final JoinableFileManager mgr = new JoinableFileManager();

    private String content;

    private File file;

    private final Logger logger = LoggerFactory.getLogger( getClass() );

    @Rule
    public TestRule timeout = ThreadDumper.timeoutRule( 2, TimeUnit.MINUTES );

    @Before
    public void setup()
            throws IOException
    {
        file = temp.newFile( "testfile.txt" );

        mgr.startReporting();

        StringBuilder contentBuilder = new StringBuilder();
        for ( int i = 0; i < COUNT; i++ )
        {
            contentBuilder.append( System.currentTimeMillis() ).append( "\n" );
        }

        content = contentBuilder.toString();
    }

    @Test
    public void run()
            throws Exception
    {
        final List<Long> timings = new ArrayList<>();
        for ( int i = 0; i < ITERATIONS; i++ )
        {
            long start = System.currentTimeMillis();
            executeTestIteration();
            long end = System.currentTimeMillis();
            timings.add( end - start );
        }

        long total = 0;
        for ( Long time : timings )
        {
            total += time;
        }

        logger.info( "Average: {}ms (per-thread: {}ms)", ( total / timings.size() ),
                     ( total / ( timings.size() * THREADS ) ) );
    }

    private void executeTestIteration()
            throws Exception
    {
        ThreadContext.getContext( true );

        ExecutorCompletionService<String> completionService = new ExecutorCompletionService<String>( executor );

        final AtomicBoolean readFlag = new AtomicBoolean( false );
        final AtomicBoolean writeFlag = new AtomicBoolean( false );

        completionService.submit( writer( writeFlag, readFlag ) );
        for ( int i = 0; i < THREADS; i++ )
        {
            completionService.submit( reader( readFlag ) );
        }

        writeFlag.set( true );

        for ( int i=0; i<(THREADS+1); i++)
        {
            String error = completionService.take().get();
            if ( error != null )
            {
                logger.info( error );
                fail( "thread failed.");
            }
            assertThat( error, nullValue() );
        }

        ThreadContext.clearContext();
    }

    private Callable<String> reader( final AtomicBoolean readFlag )
    {
        return () ->
        {
            String error = null;
            synchronized ( this )
            {
                while ( !readFlag.get() )
                {
                    try
                    {
//                            logger.info(
//                                    "Waiting for readMutex notification before reading test file..." );
                        wait( 10 );
                    }
                    catch ( InterruptedException e )
                    {
                        e.printStackTrace();
                        error = "Failed to wait for readMutex";
                    }
                }
            }

            if ( error != null )
            {
                return error;
            }

            logger.info( "Reading test file" );
            try (InputStream stream = mgr.openInputStream( file ))
            {
                String result = IOUtils.toString( stream );
                if ( !content.equals( result ) )
                {
                    error = String.format( "Content mismatch!\nExpected:\n\n%s\n\nActual:\n\n%s", content, result );
                }
            }
            catch ( IOException | InterruptedException e )
            {
                e.printStackTrace();
                error = "Failed to write test file.";
            }

            return error;
        };
    }

    private Callable<String> writer( final AtomicBoolean writeFlag, final AtomicBoolean readFlag )
    {
        return () ->
        {
            String error = null;
            synchronized ( this )
            {
                while ( !writeFlag.get() )
                {
                    try
                    {
                        wait(10);
                    }
                    catch ( InterruptedException e )
                    {
                        e.printStackTrace();
                        error = "Failed to wait for writeMutex";
                    }
                }
            }

            if ( error != null )
            {
                return error;
            }

            logger.info( "Writing test file" );
            byte[] data = content.getBytes();
            try (OutputStream stream = mgr.openOutputStream( file ))
            {
                for ( int i = 0; i < data.length; i++ )
                {
                    //                    logger.info( "write: {}", i );
                    stream.write( data[i] );
                    if ( WAIT > 0 )
                    {
                        synchronized ( this )
                        {
                            wait( WAIT );
                        }
                    }

                    // wait for there to be something in the buffer.
                    if ( i < 1 )
                    {
                        logger.info( "Notifying readMutex" );
                        readFlag.set( true );
                    }
                }

                logger.info( "Write complete. Closing output stream." );
            }
            catch ( IOException | InterruptedException e )
            {
                e.printStackTrace();
                error = "Failed to write test file";
            }

            return error;
        };
    }
}
