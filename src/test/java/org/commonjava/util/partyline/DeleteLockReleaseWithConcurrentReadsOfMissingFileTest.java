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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.commonjava.util.partyline.UtilThreads.deleter;
import static org.commonjava.util.partyline.UtilThreads.reader;
import static org.commonjava.util.partyline.UtilThreads.writer;
import static org.commonjava.util.partyline.fixture.ThreadDumper.timeoutRule;

public class DeleteLockReleaseWithConcurrentReadsOfMissingFileTest
        extends AbstractJointedIOTest
{
    @Rule
    public TestRule timeout = timeoutRule( 30, TimeUnit.SECONDS );

    /**
     * Test that locks for deletion and then checks that locks associated with mutiple read FAILURES clear correctly.
     * This will setup an intricate script of events for a single file, where:
     * <ol>
     *     <li>Deletion happens first</li>
     *     <li>Multiple reads happen simultaneously, and fail because the file doesn't exist</li>
     *     <li>A single write at the end ensures the other locks are clear</li>
     * </ol>
     * @throws Exception
     */
    @Test
    public void run()
            throws Exception
    {
        final ExecutorService execs = Executors.newFixedThreadPool( 5 );
        final File f = temp.newFile( "child.txt" );

        final CountDownLatch masterLatch = new CountDownLatch( 5 );
        CountDownLatch beginReadLatch = new CountDownLatch( 3 );
        CountDownLatch endReadLatch = new CountDownLatch( 3 );
        CountDownLatch endDeleteLatch = new CountDownLatch( 1 );

        final JoinableFileManager manager = new JoinableFileManager();

        execs.execute( writer( manager, f, masterLatch, endReadLatch ) );

        for ( int i = 0; i < 3; i++ )
        {
            final int k = i;
            execs.execute( reader( k, manager, f, masterLatch, endReadLatch, beginReadLatch, endDeleteLatch ) );
        }

        execs.execute( deleter( manager, f, masterLatch, endDeleteLatch ) );

        masterLatch.await();
    }

}
