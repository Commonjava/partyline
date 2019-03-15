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

import org.commonjava.util.partyline.lock.LockLevel;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitConfig;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

@RunWith( BMUnitRunner.class )
public class LockFileOpenOutputStreamWaitsForUnlockTest
        extends AbstractBytemanTest
{
    /**
     * Simulate the condition after releasing the write-lock on a file, another writing on the file is able to be proceeded.
     * this setup an script of events for one single file, where:
     * <ol>
     *     <li>Lock and then Unlock on a specific file</li>
     *     <li>Then proceed the writing on this file</li>
     * </ol>
     * @throws Exception
     */
    @BMRules( rules = {
            // wait for lockUnlock call to exit
            @BMRule( name = "openOutputStream", targetClass = "Partyline",
                     targetMethod = "openOutputStream(File,long)",
                     targetLocation = "ENTRY",
                     //condition = "$2==-1",
                     action = "debug(\">>>wait for service enter lockUnlock.\");" + "waitFor(\"lockUnlock\");"
                             + "debug(\"<<<proceed with openOutputStream.\")" ),

            // setup the trigger to signal openOutputStream when the lockUnlock exits
            @BMRule( name = "lockUnlock", targetClass = "Partyline",
                     targetMethod = "unlock",
                     targetLocation = "EXIT",
                     //condition = "$2.equals(\"test\")",
                     action = "debug(\"<<<signalling openOutputStream.\"); " + "signalWake(\"lockUnlock\", true);"
                             + "debug(\"<<<signalled openOutputStream.\")" ) } )

    @Test
    @BMUnitConfig( debug = true )
    public void run()
            throws Exception
    {
        final Partyline manager = getPartylineInstance();

        final File f = temp.newFile( "test.txt" );
        final String lockUnlock = "lock-clearLocks";
        final String output = "output";

        Map<String, Runnable> executions = new LinkedHashMap<>();
        executions.put( output, () -> {
            Thread.currentThread().setName( output );
            try
            {
                OutputStream o = manager.openOutputStream( f, 60000 );
                o.close();
            }
            catch ( Exception e )
            {
                e.printStackTrace();
                fail( "Failed to open outputStream: " + e.getMessage() );
            }
        } );

        executions.put( lockUnlock, () -> {
            Thread.currentThread().setName( lockUnlock );
            try
            {
                final boolean locked = manager.lock( f, 100, LockLevel.write, "test" );
                assertThat( locked, equalTo( true ) );
                assertThat( manager.unlock( f ), equalTo( true ) );
            }
            catch ( final InterruptedException e )
            {
                fail( "Interrupted!" );
            }
            catch ( IOException e )
            {
                e.printStackTrace();
                fail( "Failed to unlock: " + e.getMessage() );
            }
        } );

        assertThat( raceExecutions( executions ), equalTo( lockUnlock ) );
    }
}
