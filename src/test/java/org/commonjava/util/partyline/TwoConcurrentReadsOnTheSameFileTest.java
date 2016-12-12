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
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitConfig;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

@RunWith( BMUnitRunner.class )
public class TwoConcurrentReadsOnTheSameFileTest
        extends AbstractBytemanTest
{
    /**
     * Test that verifies concurrent reading tasks on the same file are allowable, this setup an script of events for
     * one single file, where:
     * <ol>
     *     <li>Multiple reads happen simultaneously, read the content</li>
     *     <li>Simulate reading time as 1s before stream close</li>
     *     <li>Reading processes on the same file have no interaction between each other</li>
     * </ol>
     * @throws Exception
     */
    @BMRules( rules = {
            // setup the rendezvous for all reading threads, which will mean suspending everything until all threads are started.
            @BMRule( name = "init rendezvous", targetClass = "JoinableFileManager",
                     targetMethod = "<init>",
                     targetLocation = "ENTRY",
                     action = "createRendezvous(\"begin\", 2);" + "debug(\"<<<init rendezvous for begin.\")" ),

            // setup the rendezvous to wait for all threads to be ready before proceeding.
            @BMRule( name = "openInputStream start", targetClass = "JoinableFileManager",
                     targetMethod = "openInputStream",
                     targetLocation = "ENTRY",
                     action = "debug(\">>>Waiting for ALL to start.\");" + "rendezvous(\"begin\");"
                             + "debug(\"<<<\"+Thread.currentThread().getName() + \": openInputStream() thread proceeding.\" )" ),

            // hold inputStream waiting for 1s before its close
            @BMRule( name = "hold closed", targetClass = "JoinableFile$JoinInputStream",
                     targetMethod = "close",
                     targetLocation = "ENTRY",
                     action = "debug(\">>>waiting for closed.\");" + "java.lang.Thread.sleep(1000);" ) } )
    @Test
    @BMUnitConfig( debug = true )
    public void run()
            throws Exception
    {
        final ExecutorService execs = Executors.newFixedThreadPool( 2 );
        final CountDownLatch latch = new CountDownLatch( 2 );
        final JoinableFileManager manager = new JoinableFileManager();

        final String content = "This is a bmunit test";
        final File file = temp.newFile( "file_both_read.txt" );

        FileUtils.write( file, content );

        List<String> returning = new ArrayList<String>();
        for ( int i = 0; i < 2; i++ )
        {
            final int k = i;

            execs.execute( () -> {
                Thread.currentThread().setName( "openInputStream-" + k );
                try (InputStream s = manager.openInputStream( file ))
                {
                    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    int read = -1;
                    final byte[] buf = new byte[512];
                    System.out.println( String.format(
                            "<<<concurrent reading>>> will start to read from the resource with inputStream %s",
                            s.getClass().getName() ) );
                    while ( ( read = s.read( buf ) ) > -1 )
                    {
                        baos.write( buf, 0, read );
                    }

                    baos.close();
                    s.close();
                    System.out.println( String.format(
                            "<<<concurrent reading>>> reading from the resource done with inputStream %s",
                            s.getClass().getName() ) );

                    returning.add( new String( baos.toByteArray(), "UTF-8" ) );
                }
                catch ( Exception e )
                {
                    e.printStackTrace();
                    fail( "Failed to open inputStream: " + e.getMessage() );
                }
                finally
                {
                    latch.countDown();
                }
            } );
        }

        latch.await();

        assertThat( returning.get( 0 ), equalTo( content ) );
        assertThat( returning.get( 1 ), equalTo( content ) );
    }
}
