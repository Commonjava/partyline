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
public class ReadLockOnDerivativeDontPreventMainFileReadTest
        extends AbstractJointedIOTest
{
    private final ExecutorService testPool = Executors.newFixedThreadPool( 2 );

    private final CountDownLatch latch = new CountDownLatch( 2 );

    private final JoinableFileManager fileManager = new JoinableFileManager();

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    @Test
    @BMScript( "ReadLockOnDerivativeSiblingFile_DontPreventMainFileRead.btm" )
    public void readLockOnDerivativeSiblingFile_DontPreventMainFileRead()
            throws Exception
    {
        final String mContent = "main";
        final String dContent = "derivative";

        final File d = temp.newFolder();
        final File main = new File( d, "org/foo/bar/1/bar-1.pom" );
        final File derivative = new File( d, "org/foo/bar/1/bar-1.pom.sha1" );

        FileUtils.write( main, mContent );
        FileUtils.write( derivative, dContent );

        final Future<String> derivativeReading = testPool.submit(
                (Callable<String>) new OpenInputStreamTask( fileManager, dContent, derivative, latch ) );
        final Future<String> mainReading =
                testPool.submit( (Callable<String>) new OpenInputStreamTask( fileManager, mContent, main, latch ) );

        latchWait( latch );

        final String mainStream = mainReading.get();
        assertThat( mainStream, equalTo( mContent ) );
    }

}
