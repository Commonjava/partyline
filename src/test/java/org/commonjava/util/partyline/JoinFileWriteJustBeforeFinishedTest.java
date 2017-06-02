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

import org.commonjava.util.partyline.fixture.TimedFileWriter;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitConfig;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@RunWith( BMUnitRunner.class )
public class JoinFileWriteJustBeforeFinishedTest
        extends AbstractBytemanTest
{
    /**
     * Test verifies JoinableFile read could be done before its write stream close
     * with read init delay, this setup an script of events for one single file, where:
     * <ol>
     *     <li>Simulate JoinableFile write process </li>
     *     <li>Read should be proceeded before write stream close</li>
     * </ol>
     * @throws Exception
     */
    @BMRules( rules = {
            // wait for read call to exit
            @BMRule( name = "write close", targetClass = "JoinableFile",
                     targetMethod = "close",
                     targetLocation = "ENTRY",
                     condition = "incrementCounter($0)==1",
                     action = "debug(\">>>wait for service enter read.\");" + "waitFor(\"read\");"
                             + "debug(\"<<<proceed with write close.\")" ),

            // setup the trigger to signal write close when the read exits
            @BMRule( name = "read", targetClass = "JoinableFile",
                     targetMethod = "joinStream",
                     targetLocation = "EXIT",
                     action = "debug(\"<<<signalling write close.\"); " + "signalWake(\"read\", true);"
                             + "debug(\"<<<signalled write close.\")" ) } )
    @Test
    @BMUnitConfig( debug = true )
    public void run()
            throws Exception
    {
        final ExecutorService execs = Executors.newFixedThreadPool( 2 );
        final CountDownLatch latch = new CountDownLatch( 2 );

        final File file = temp.newFile();
        String threadName = "writer" + writers++;

        final JoinableFile stream =
                new JoinableFile( file, new LockOwner( file.getAbsolutePath(), name.getMethodName(), LockLevel.write ), true );

        execs.execute( () -> {
            Thread.currentThread().setName( threadName );
            new TimedFileWriter( stream, 1, latch ).run();
        } );

        execs.execute( () -> {
            Thread.currentThread().setName( "reader" + readers++ );
            new AsyncFileReader( 1000, -1, -1, stream, latch ).run();
        } );

        System.out.println( "Waiting for " + name.getMethodName() + " threads to complete." );
        latch.await();
    }
}
