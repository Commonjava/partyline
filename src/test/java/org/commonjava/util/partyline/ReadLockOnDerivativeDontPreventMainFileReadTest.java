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
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitConfig;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

@RunWith( BMUnitRunner.class )
public class ReadLockOnDerivativeDontPreventMainFileReadTest
        extends AbstractBytemanTest
{
    /**
     * Test that verifies concurrent reading locks on different files will not effect each other's reading process,
     * this setup an script of events for multiple files, where:
     * <ol>
     *     <li>Multiple reads happen simultaneously, read locks on distinct files/li>
     *     <li>Reading processes for different files are isolated</li>
     * </ol>
     * @throws Exception
     */
    @BMRules( rules = {
            // wait for first openInputStream call to exit
            @BMRule( name = "second openInputStream", targetClass = "Partyline",
                     targetMethod = "openInputStream",
                     targetLocation = "ENTRY",
                     binding = "name:String = $1.getName()",
                     condition = "name.equals(\"bar-1.pom\")",
                     action = "debug(\">>>wait for service enter first openInputStream.\");"
                             + "waitFor(\"first openInputStream\");"
                             + "debug(\"<<<proceed with second openInputStream.\")" ),

            // setup the trigger to signal second openInputStream when the first openInputStream exits
            @BMRule( name = "first openInputStream", targetClass = "Partyline",
                     targetMethod = "openInputStream",
                     targetLocation = "ENTRY",
                     binding = "name:String = $1.getName()",
                     condition = "name.equals(\"bar-1.pom.sha1\")",
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
        final Partyline manager = getPartylineInstance();

        final String main = "main";
        final String derivative = "derivative";

        final File d = temp.newFolder();
        final File mFile = new File( d, "org/foo/bar/1/bar-1.pom" );
        final File dFile = new File( d, "org/foo/bar/1/bar-1.pom.sha1" );

        FileUtils.write( mFile, main );
        FileUtils.write( dFile, derivative );

        Map<String, String> returning = new HashMap<String, String>();

        for ( int i = 0; i < 2; i++ )
        {
            final int k = i;
            execs.execute( () -> {
                File file = null;
                String name = "";

                switch ( k )
                {
                    case 0:
                        file = mFile;
                        name = main;
                        break;
                    case 1:
                        file = dFile;
                        name = derivative;
                        break;
                }
                Thread.currentThread().setName( name );
                try (InputStream s = manager.openInputStream( file ))
                {
                    returning.put( name, IOUtils.toString( s ) );
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
            Thread.sleep( 1000 ); // make the fragile BMRule to always work
        }

        latch.await();

        // note reporting main null error
        final String mainStream = returning.get( main );
        assertThat( mainStream, equalTo( main ) );
    }

}
