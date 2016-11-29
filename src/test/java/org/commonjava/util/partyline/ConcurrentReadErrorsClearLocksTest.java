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
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitConfig;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@RunWith( BMUnitRunner.class )
public class ConcurrentReadErrorsClearLocksTest
        extends AbstractJointedIOTest
{
    /**
     * Test that locks for mutiple reads clear correctly. This will setup an script of events for
     * a single file, where:
     * <ol>
     *     <li>Multiple reads happen simultaneously, read the content, and close</li>
     *     <li>A single write at the end ensures the other locks are clear</li>
     * </ol>
     * @throws Exception
     */
    /*@formatter:off*/
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
                     action = "debug(\"Waiting for READ to start.\"); "
                             + "rendezvous(\"begin\"); "
                             + "waitFor(\"reading\"); "
                             + "debug(Thread.currentThread().getName() + \": openInputStream() thread proceeding.\")" ),

            // setup the rendezvous to wait for all threads to be ready before proceeding
            @BMRule( name = "openInputStream start", targetClass = "JoinableFileManager",
                     targetMethod = "openInputStream",
                     targetLocation = "ENTRY",
                     action = "debug(\"Waiting for ALL to start.\"); "
                             + "rendezvous(\"begin\"); "
                             + "debug(Thread.currentThread().getName() + \": openInputStream() thread proceeding.\")" ),

            // setup the trigger to signal openOutputStream when the first openInputStream exits
            @BMRule( name = "openInputStream end", targetClass = "JoinableFileManager",
                     targetMethod = "openInputStream",
                     targetLocation = "EXCEPTION EXIT",
                     action = "debug(\"Signal READ.\"); "
                             + "signalWake(\"reading\"); "
                             + "debug(Thread.currentThread().getName() + \": openInputStream() done.\")" ),

            // When we try to init a new JoinableFile for INPUT, simulate an IOException from somewhere deeper in the stack.
            @BMRule( name = "new JoinableFile error", targetClass = "JoinableFile", targetMethod = "<init>",
                     targetLocation = "ENTRY",
                     condition = "$4 == false",
                     action = "debug(\"Throwing test exception.\"); "
                             + "throw new java.io.IOException(\"Test exception\")" ) } )
    /*@formatter:on*/
    @BMUnitConfig( debug = true )
    @Test
    public void run()
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

        latch.await();
    }

}
