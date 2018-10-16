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

import org.junit.Test;

import java.io.File;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.fail;

public class VerifyReportingDaemonWorksTest
        extends AbstractJointedIOTest
{
    @Test
    public void run()
            throws Exception
    {
        final ExecutorService execs = Executors.newFixedThreadPool( 2 );
        final CountDownLatch latch = new CountDownLatch( 1 );
        final JoinableFileManager manager = new JoinableFileManager();

        final File f = temp.newFile();
        final String first = "first";

        manager.startReporting( 0, 1000 );

        List<String> returning = new ArrayList<>();

        execs.execute( () -> {
            Thread.currentThread().setName( "reportingDaemonThread" );
            try
            {
                OutputStream o = manager.openOutputStream( f );
                returning.add( String.valueOf( System.nanoTime() ) );
                Thread.sleep( 6000 );
                o.close();
            }
            catch ( Exception e )
            {
                e.printStackTrace();
                fail( "Failed to open outputStream: " + e.getMessage() );
            }
            finally
            {
                latch.countDown();
            }

        } );
        latch.await();

        long completed = Long.valueOf( returning.get( 0 ) );
        System.out.println( first + " completed at: " + completed );
    }
}
