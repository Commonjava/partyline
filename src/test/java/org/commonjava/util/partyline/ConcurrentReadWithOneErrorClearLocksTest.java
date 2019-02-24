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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.commonjava.util.partyline.UtilThreads.reader;
import static org.commonjava.util.partyline.UtilThreads.writer;

@RunWith( BMUnitRunner.class )
public class ConcurrentReadWithOneErrorClearLocksTest
        extends AbstractBytemanTest
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
                     action = "createCountDown(\"JOIN\", 2)" ),

            // When we try to init a new JoinableFile for INPUT, simulate an IOException from somewhere deeper in the stack.
            @BMRule( name = "new JoinableFile error", targetClass = "JoinableFile", targetMethod = "joinStream",
                     targetLocation = "ENTRY",
                     condition = "countDown(\"JOIN\")",
                     action = "debug(\"Throwing test exception in \" + Thread.currentThread().getName()); "
                             + "throw new java.io.IOException(\"Test exception\")" ) } )
    /*@formatter:on*/
    @Test
    @BMUnitConfig( debug = true )
    //    @Ignore( "Inconsistent result between Maven/IDEA executions; needs to be fixed before release!" )
    public void run()
            throws Exception
    {
        final ExecutorService execs = Executors.newFixedThreadPool( 5 );
        final File f = temp.newFile( "child.txt" );
        FileUtils.write( f, "test data" );

        final CountDownLatch latch = new CountDownLatch( 4 );
        CountDownLatch readBeginLatch = new CountDownLatch( 3 );
        CountDownLatch readEndLatch = new CountDownLatch( 3 );

        final Partyline manager = getPartylineInstance();
        manager.startReporting( 5000, 5000 );
        final long start = System.currentTimeMillis();

        execs.execute( writer( manager, f, latch, readEndLatch ) );

        for ( int i = 0; i < 3; i++ )
        {
            final int k = i;
            execs.execute( reader(k, manager, f, latch, readBeginLatch, readEndLatch, null) );
        }

        latch.await();
    }

}
