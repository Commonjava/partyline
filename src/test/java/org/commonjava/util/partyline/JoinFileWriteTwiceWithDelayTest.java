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

import org.commonjava.cdi.util.weft.SignallingLock;
import org.commonjava.util.partyline.fixture.TimedFileWriter;
import org.commonjava.util.partyline.impl.local.RandomAccessJFS;
import org.commonjava.util.partyline.lock.LockLevel;
import org.commonjava.util.partyline.lock.local.LocalLockOwner;
import org.commonjava.util.partyline.spi.JoinableFile;
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
public class JoinFileWriteTwiceWithDelayTest
        extends AbstractBytemanTest
{
    /**
     * Test verifies JoinableFile simultaneous reads could be done before its write stream close,
     * this setup an script of events for one single file, where:
     * <ol>
     *     <li>Simulate JoinableFile write process </li>
     *     <li>Two simultaneous reads with init delay should be proceeded before write stream close</li>
     * </ol>
     * @throws Exception
     */
    @BMRules( rules = {
            // setup the rendezvous for all threads, which will mean suspending everything until all threads are started.
            @BMRule( name = "init rendezvous", targetClass = "JoinableFile",
                     targetMethod = "<init>",
                     targetLocation = "ENTRY",
                     action = "createRendezvous(\"begin\", 3);" + "debug(\"<<<init rendezvous for begin.\")" ),

            // setup the rendezvous to wait for all threads to be ready before proceeding.
            @BMRule( name = "write close", targetClass = "JoinableFile",
                     targetMethod = "close",
                     targetLocation = "ENTRY",
                     condition = "incrementCounter($0)==1",
                     action = "debug(\">>>Waiting for ALL to start.\");" + "rendezvous(\"begin\");"
                             + "debug(\"<<<\"+Thread.currentThread().getName() + \": write thread proceeding.\" )" ),

            @BMRule( name = "read", targetClass = "JoinableFile",
                     targetMethod = "joinStream",
                     targetLocation = "EXIT",
                     action = "debug(\">>>Waiting for ALL to start.\");" + "rendezvous(\"begin\");"
                             + "debug(\"<<<\"+Thread.currentThread().getName() + \": read thread proceeding.\" )" ) } )
    @Test
    @BMUnitConfig( debug = true )
    public void run()
            throws Exception
    {
        final ExecutorService execs = Executors.newFixedThreadPool( 3 );
        final CountDownLatch latch = new CountDownLatch( 3 );

        final File file = temp.newFile();
        String threadName = "writer" + writers++;

        final JoinableFile stream =
                new RandomAccessJFS().getFile( file, new LocalLockOwner( file.getAbsolutePath(), name.getMethodName(), LockLevel.write ), null, true,
                                               new SignallingLock() );

        execs.execute( () -> {
            Thread.currentThread().setName( threadName );
            new TimedFileWriter( stream, 1, latch ).run();
        } );

        execs.execute( () -> {
            Thread.currentThread().setName( "reader" + readers++ );
            new AsyncFileReader( 0, -1, -1, stream, latch ).run();
        } );

        execs.execute( () -> {
            Thread.currentThread().setName( "reader" + readers++ );
            new AsyncFileReader( 500, -1, -1, stream, latch ).run();
        } );

        System.out.println( "Waiting for " + name.getMethodName() + " threads to complete." );
        latch.await();
    }

}
