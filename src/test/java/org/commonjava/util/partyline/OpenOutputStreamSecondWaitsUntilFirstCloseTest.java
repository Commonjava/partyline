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

import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitConfig;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.OutputStream;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

@RunWith( BMUnitRunner.class )
public class OpenOutputStreamSecondWaitsUntilFirstCloseTest
        extends AbstractBytemanTest
{

    /**
     * Test aligns two concurrent writing tasks as second starts until first close, operated on the same file
     * verified to be available, this setup an script of events for one single file, where:
     * <ol>
     *     <li>Multiple writes happened as a specific sequence</li>
     *     <li>Has no simultaneous Writing lock on the same file</li>
     * </ol>
     * @throws Exception
     */
    @BMRules( rules = {
            // wait for first openOutputStream call to exit
            @BMRule( name = "second openOutputStream", targetClass = "Partyline",
                     targetMethod = "openOutputStream(File,long)",
                     targetLocation = "ENTRY",
                     condition = "$2==100",
                     action = "debug(\">>>wait for service enter first openOutputStream.\");"
                             + "waitFor(\"first openOutputStream\");" + "java.lang.Thread.sleep(100);"
                             + "debug(\"<<<proceed with second openOutputStream.\")" ),

            // setup the trigger to signal second openOutputStream when the first openOutputStream exits
            @BMRule( name = "first openOutputStream", targetClass = "Partyline",
                     targetMethod = "openOutputStream(File,long)",
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
        final Partyline manager = getPartylineInstance();


        final File f = temp.newFile();
        final String first = "first";
        final String second = "second";

        Map<String, Runnable> executions = new LinkedHashMap<>();

        for ( int i = 0; i < 2; i++ )
        {
            final int k = i;
            String tname = k < 1 ? first : second;
            executions.put(tname, () -> {

                Thread.currentThread().setName( tname );
                OutputStream o = null;
                try
                {
                    switch ( k )
                    {
                        case 0:
                            o = manager.openOutputStream( f, -1 );
                            break;
                        case 1:
                            o = manager.openOutputStream( f, 100 );
                    }
                    o.write( "Test data".getBytes() );
                    o.close();
                }
                catch ( Exception e )
                {
                    e.printStackTrace();
                    fail( "Failed to open outputStream: " + e.getMessage() );
                }
            } );
        }

        assertThat( raceExecutions( executions ), equalTo( first ) );
    }

}
