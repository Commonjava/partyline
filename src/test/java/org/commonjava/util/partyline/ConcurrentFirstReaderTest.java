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
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitConfig;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.commonjava.util.partyline.UtilThreads.reader;
import static org.commonjava.util.partyline.UtilThreads.writer;

@RunWith( BMUnitRunner.class )
public class ConcurrentFirstReaderTest
        extends AbstractJointedIOTest
{
    /**
     * GIVEN:
     * <ol>
     *     <li>File exists on disk</li>
     *     <li>File is not opened for reading or writing</li>
     *     <li>The underlying filesystem is slow to respond to lock requests on FileChannel</li>
     * </ol>
     * <br/>
     * WHEN:
     * <ol>
     *     <li>Two reader threads attempt to open the file at the same time</li>
     * </ol>
     * <br/>
     * THEN:
     * <ol>
     *     <li>One reader thread should "win" and be the first to open the file</li>
     *     <li>The other reader thread should "join" that first thread's open file</li>
     *     <li>No OverlappingFileLockException should be thrown</li>
     * </ol>
     * @throws Exception
     */
    /*@formatter:off*/
    @BMRules( rules = {
            // setup a rendezvous to control thread execution
            @BMRule( name = "init rendezvous", targetClass = "FileTree",
                     targetMethod = "<init>",
                     targetLocation = "ENTRY",
                     action = "createRendezvous(\"begin\", 2);" + "debug(\"<<<init rendezvous for begin.\")" ),
            // sync up to reduce chances of missing the race condition for the opLock.lock() control.
            @BMRule( name="sync FileTree.setOrJoin", targetClass = "FileTree",
                     targetMethod = "setOrJoinFile",
                     targetLocation = "ENTRY",
                     action="debug(\"Rendezvous read operations.\"); "
                             + "rendezvous(\"begin\"); "
                             + "debug(\"Continue read operations.\");"),
            // When we try to init a new JoinableFile for INPUT, simulate an IOException from somewhere deeper in the stack.
            @BMRule( name = "new JoinableFile lock delay", targetClass = "JoinableFile", targetMethod = "<init>",
                     targetLocation = "ENTRY",
                     action = "debug(\"Delaying JoinableFile.init. Lock is: \" + $5); "
                             + "Thread.sleep(500);"
                             + "debug( \"Resuming lock operation.\" );" ) } )
    /*@formatter:on*/
    @BMUnitConfig( debug = true )
    @Test
    public void run()
            throws Exception
    {
        byte[] bytes = new byte[1024*1024*2]; // let's get some serious data going.

        Random rand = new Random();
        rand.nextBytes( bytes );

        final ExecutorService execs = Executors.newFixedThreadPool( 2 );
        final File f = temp.newFile( "child.bin" );
        FileUtils.writeByteArrayToFile( f, bytes );

        final CountDownLatch latch = new CountDownLatch( 2 );
        final JoinableFileManager manager = new JoinableFileManager();

        manager.startReporting( 5000, 5000 );

        for ( int i = 0; i < 2; i++ )
        {
            final int k = i;
            execs.execute( reader( k, manager, f, latch, true ) );
        }

        latch.await();
    }

}
