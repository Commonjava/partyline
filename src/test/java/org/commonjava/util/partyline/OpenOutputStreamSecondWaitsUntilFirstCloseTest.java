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

import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitConfig;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

@RunWith( BMUnitRunner.class )
public class OpenOutputStreamSecondWaitsUntilFirstCloseTest
        extends AbstractBytemanTest
{

    /**
     * Test aligns two concurrent writing tasks as second starts until first close, operated on the same file verified to be available, this setup an script of events for
     * one single file, where:
     * <ol>
     *     <li>Multiple writes happened as a specific sequence</li>
     *     <li>Has no simultaneous Writing lock on the same file</li>
     * </ol>
     * @throws Exception
     */
    @BMRules( rules = {
            // wait for first openOutputStream call to exit
            @BMRule( name = "second openOutputStream", targetClass = "JoinableFileManager",
                     targetMethod = "openOutputStream",
                     targetLocation = "ENTRY",
                     condition = "$2==100",
                     action = "debug(\">>>wait for service enter first openOutputStream.\");"
                             + "waitFor(\"first openOutputStream\");" + "java.lang.Thread.sleep(10);"
                             + "debug(\"<<<proceed with second openOutputStream.\")" ),

            // setup the trigger to signal second openOutputStream when the first openOutputStream exits
            @BMRule( name = "first openOutputStream", targetClass = "JoinableFileManager",
                     targetMethod = "openOutputStream",
                     targetLocation = "EXIT",
                     condition = "$2==-1",
                     action = "debug(\"<<<signalling second openOutputStream.\"); "
                             + "signalWake(\"first openOutputStream\", true);"
                             + "debug(\"<<<signalled second openOutputStream.\")" ) } )
    @Test
    @BMUnitConfig( debug = true )
    public void run()
            throws Exception
    {
        final ExecutorService execs = Executors.newFixedThreadPool( 2 );
        final CountDownLatch latch = new CountDownLatch( 2 );
        final JoinableFileManager manager = new JoinableFileManager();

        final File f = temp.newFile();
        final String first = "first";
        final String second = "second";

        Map<String, String> returning = new HashMap<String, String>();

        for ( int i = 0; i < 2; i++ )
        {
            final int k = i;
            execs.execute( () -> {

                Thread.currentThread().setName( "openOutputStream-" + k );
                OutputStream o = null;
                try
                {
                    switch ( k )
                    {
                        case 0:
                            o = manager.openOutputStream( f, -1 );
                            returning.put( first, String.valueOf( System.nanoTime() ) );
                            break;
                        case 1:
                            o = manager.openOutputStream( f, 100 );
                            returning.put( second, String.valueOf( System.nanoTime() ) );
                    }
                    o.write( "Test data".getBytes() );
                    o.close();
                }
                catch ( Exception e )
                {
                    e.printStackTrace();
                    fail( "Failed to open outputStream: " + e.getMessage() );
                }
                finally
                {
                    latch.countDown();
                }

            } );
        }

        latch.await();

        long fistTimestamp = Long.valueOf( returning.get( first ) );
        long secondTimestamp = Long.valueOf( returning.get( second ) );
        assertThat( first + " completed at: " + fistTimestamp + "\n" + second + " completed at: " + secondTimestamp
                            + "\nFirst should complete before second.", fistTimestamp < secondTimestamp,
                    equalTo( true ) );
    }

}
