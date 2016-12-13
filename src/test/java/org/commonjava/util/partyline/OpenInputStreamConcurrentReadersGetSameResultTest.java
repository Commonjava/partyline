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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

@RunWith( BMUnitRunner.class )
public class OpenInputStreamConcurrentReadersGetSameResultTest
        extends AbstractBytemanTest
{
    /**
     * Test that verifies concurrent reading tasks, including timeout one, on the same file are allowable, this setup an script of events for
     * one single file, where:
     * <ol>
     *     <li>Multiple reads happen simultaneously, read the content</li>
     *     <li>Reading processes on the same file have no interaction between each other</li>
     * </ol>
     * @throws Exception
     */
    @BMRules( rules = {
            // wait for first openInputStream call to exit
            @BMRule( name = "second openInputStream", targetClass = "JoinableFileManager",
                     targetMethod = "openInputStream",
                     targetLocation = "ENTRY",
                     condition = "$2==10",
                     action = "debug(\">>>wait for service enter first openInputStream.\");"
                             + "waitFor(\"first openInputStream\");"
                             + "debug(\"<<<proceed with second openInputStream.\")" ),

            // setup the trigger to signal second openInputStream when the first openInputStream exits
            @BMRule( name = "first openInputStream", targetClass = "JoinableFileManager",
                     targetMethod = "openInputStream",
                     targetLocation = "ENTRY",
                     condition = "$2==-1",
                     action = "debug(\"<<<signalling second openInputStream.\"); "
                             + "signalWake(\"first openInputStream\", true);"
                             + "debug(\"<<<signalled second openInputStream.\")" ) } )

    @Test
    @BMUnitConfig( debug = true )
    public void run()
            throws Exception
    {
        final ExecutorService execs = Executors.newFixedThreadPool( 2 );
        final CountDownLatch latch = new CountDownLatch( 2 );
        final JoinableFileManager manager = new JoinableFileManager();

        final File f = temp.newFile();
        String str = "This is a test";
        FileUtils.write( f, str );

        List<String> returning = new ArrayList<String>();

        for ( int i = 0; i < 2; i++ )
        {
            final int k = i;
            execs.execute( () -> {
                Thread.currentThread().setName( "openInputStream-" + k );
                InputStream s = null;
                try
                {
                    switch ( k )
                    {
                        case 0:
                            s = manager.openInputStream( f, -1 );
                            break;
                        case 1:
                            // note reporting timeout handle error
                            s = manager.openInputStream( f, 10 );
                            break;
                    }
                    returning.add( IOUtils.toString( s ) );
                    s.close();
                }
                catch ( final Exception e )
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

        assertThat( returning.get( 0 ), equalTo( str ) );
        assertThat( returning.get( 1 ), equalTo( str ) );
    }
}
