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

import org.jboss.byteman.contrib.bmunit.BMScript;
import org.jboss.byteman.contrib.bmunit.BMUnitConfig;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.File;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

@RunWith( org.jboss.byteman.contrib.bmunit.BMUnitRunner.class )
@BMUnitConfig( loadDirectory = "target/test-classes/bmunit", debug = true )
public class OpenOutputStreamSecondWaitsUntilFirstCloseTest
        extends AbstractJointedIOTest
{
    private final ExecutorService testPool = Executors.newFixedThreadPool( 2 );

    private final CountDownLatch latch = new CountDownLatch( 2 );

    private final JoinableFileManager fileManager = new JoinableFileManager();

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    @Test
    @BMScript( "OpenOutputStream_SecondWaitsUntilFirstCloses.btm" )
    public void openOutputStream_SecondWaitsUntilFirstCloses()
            throws Exception
    {
        final File f = temp.newFile();

        final String first = "first";
        final String second = "second";

        final Future<String> firstWriting =
                testPool.submit( (Callable<String>) new OpenOutputStreamTask( fileManager, first, f, latch, 10 ) );
        final Future<String> secondWriting =
                testPool.submit( (Callable<String>) new OpenOutputStreamTask( fileManager, second, f, latch ) );

        latchWait( latch );

        long fistTimestamp = Long.valueOf( firstWriting.get() );
        long secondTimestamp = Long.valueOf( secondWriting.get() );
        assertThat( first + " completed at: " + fistTimestamp + "\n" + second + " completed at: " + secondTimestamp
                            + "\nFirst should complete before second.", fistTimestamp < secondTimestamp,
                    equalTo( true ) );
    }

}
