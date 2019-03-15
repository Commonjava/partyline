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
import org.junit.Test;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.commonjava.util.partyline.UtilThreads.reader;
import static org.commonjava.util.partyline.UtilThreads.writer;

public class ConcurrentReadsClearLocksTest
        extends AbstractJointedIOTest
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
    @Test
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
