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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

@RunWith( org.jboss.byteman.contrib.bmunit.BMUnitRunner.class )
@BMUnitConfig( loadDirectory = "target/test-classes/bmunit", debug = true )
public class LockFileOpenOutputStreamWaitsForUnlockTest
        extends AbstractJointedIOTest
{
    private final ExecutorService testPool = Executors.newFixedThreadPool( 2 );

    private final CountDownLatch latch = new CountDownLatch( 2 );

    private final JoinableFileManager fileManager = new JoinableFileManager();

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    @Test
    @BMScript( "LockFile_OpenOutputStreamWaitsForUnlock.btm" )
    public void lockFile_OpenOutputStreamWaitsForUnlock()
            throws Exception
    {
        final File f = temp.newFile( "test.txt" );

        final String lockUnlock = "lock-clearLocks";
        final String output = "output";

        final Future<String> write =
                testPool.submit( (Callable<String>) new OpenOutputStreamTask( fileManager, output, f, latch ) );
        final Future<String> unlock =
                testPool.submit( (Callable<String>) new LockThenUnlockFile( fileManager, lockUnlock, f, latch, 100 ) );

        long writeTimestamp = Long.valueOf( write.get() );
        long unlockTimestamp = Long.valueOf( unlock.get() );

        assertThat(
                "\nLock-Unlock completed at:             " + unlockTimestamp + "\n" + "OpenOutputStream completed at: "
                        + writeTimestamp + "\nLock-Unlock should complete first", unlockTimestamp < writeTimestamp,
                equalTo( true ) );
    }

    private final class LockThenUnlockFile
            extends IOTask
            implements Callable<String>
    {

        public LockThenUnlockFile( JoinableFileManager fileManager, String content, File file,
                                   CountDownLatch controlLatch, long waiting )
        {
            super( fileManager, content, file, controlLatch, waiting );
        }

        @Override
        public void run()
        {
            Logger logger = LoggerFactory.getLogger( getClass() );
            try
            {
                logger.trace( "locking: {}", file );
                final boolean locked = fileManager.lock( file, 100, LockLevel.write, "test" );

                logger.trace( "locked? {}", locked );

                assertThat( locked, equalTo( true ) );

                logger.trace( "Waiting {}ms to unlock...", timeout );
                Thread.sleep( waiting );
            }
            catch ( final InterruptedException e )
            {
                fail( "Interrupted!" );
            }

            logger.trace( "unlocking: {}", file );
            assertThat( fileManager.unlock( file, "test" ), equalTo( true ) );
        }

        @Override
        public String call()
                throws Exception
        {
            this.run();
            return String.valueOf( System.nanoTime() );
        }
    }

}
