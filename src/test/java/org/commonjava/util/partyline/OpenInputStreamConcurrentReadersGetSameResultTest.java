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

import org.apache.commons.io.FileUtils;
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
public class OpenInputStreamConcurrentReadersGetSameResultTest
        extends AbstractJointedIOTest
{
    private final ExecutorService testPool = Executors.newFixedThreadPool( 2 );

    private final CountDownLatch latch = new CountDownLatch( 2 );

    private final JoinableFileManager fileManager = new JoinableFileManager();

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    @Test
    @BMScript( "OpenInputStreamConcurrentReadersGetSameResult.btm" )
    public void openInputStream_ConcurrentReadersGetSameResult()
            throws Exception
    {
        final File f = temp.newFile();
        String str = "This is a test";
        FileUtils.write( f, str );

        final Future<String> firstReading =
                testPool.submit( (Callable<String>) new OpenInputStreamTask( fileManager, str, f, latch ) );
        final Future<String> secondReading = testPool.submit(
                (Callable<String>) new OpenInputStreamWithTimeoutTask( fileManager, str, f, latch, 10 ) );

        latchWait( latch );

        assertThat( "first reader returned wrong data", firstReading.get(), equalTo( str ) );
        assertThat( "second reader returned wrong data", secondReading.get(), equalTo( str ) );
    }
}
