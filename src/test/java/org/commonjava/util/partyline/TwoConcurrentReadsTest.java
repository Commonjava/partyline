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
import org.apache.commons.io.IOUtils;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMScript;
import org.jboss.byteman.contrib.bmunit.BMUnitConfig;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

@RunWith( org.jboss.byteman.contrib.bmunit.BMUnitRunner.class )
@BMUnitConfig( loadDirectory = "target/test-classes/bmunit", debug = true )
public class TwoConcurrentReadsTest
        extends AbstractBytemanTest
{
    private final ExecutorService testPool = Executors.newFixedThreadPool( 2 );

    private final CountDownLatch latch = new CountDownLatch( 2 );

    private final JoinableFileManager fileManager = new JoinableFileManager();

    private void latchWait( CountDownLatch latch )
    {
        try
        {
            latch.await();
        }
        catch ( InterruptedException e )
        {
            System.out.println( "Threads await Exception." );
        }
    }

    private void writeFile( File file, String content )
            throws IOException
    {
        final OutputStream out = new FileOutputStream( file );
        IOUtils.write( content, out );
        out.close();
    }

    @Test
    @BMScript( "TryToBothRead.btm" )
    public void run()
            throws Exception
    {
        final String content = "This is a bmunit test";
        final File file = temp.newFile( "file_both_read.txt" );
        writeFile( file, content );
        final Future<String> readingFuture1 =
                testPool.submit( (Callable<String>) new ReadTask( fileManager, content, file, latch ) );
        final Future<String> readingFuture2 =
                testPool.submit( (Callable<String>) new ReadTask( fileManager, content, file, latch ) );

        latchWait( latch );

        final String readingResult1 = readingFuture1.get();
        assertThat( readingResult1, equalTo( content ) );
        final String readingResult2 = readingFuture2.get();
        assertThat( readingResult2, equalTo( content ) );
    }

}
