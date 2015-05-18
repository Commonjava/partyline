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
/*******************************************************************************
* Copyright (c) 2015 ${owner}
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the GNU Public License v3.0
* which accompanies this distribution, and is available at
* http://www.gnu.org/licenses/gpl.html
*
* Contributors:
* ${owner} - initial API and implementation
******************************************************************************/
/*******************************************************************************
* Copyright (c) 2015 Red Hat, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the GNU Public License v3.0
* which accompanies this distribution, and is available at
* http://www.gnu.org/licenses/gpl.html
*
* Contributors:
* Red Hat, Inc. - initial API and implementation
******************************************************************************/
package org.commonjava.util.partyline.fixture;

import java.io.File;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.commonjava.util.partyline.AsyncFileReader;
import org.commonjava.util.partyline.JoinableOutputStream;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

public abstract class AbstractJointedIOTest
{

    public static final int COUNT = 2000;

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    @Rule
    public TestName name = new TestName();

    protected int readers = 0;

    protected int writers = 0;

    protected int timers = 0;

    protected Thread newThread( final String named, final Runnable runnable )
    {
        final Thread t = new Thread( runnable );
        t.setName( name.getMethodName() + "::" + named );
        t.setDaemon( true );
        t.setUncaughtExceptionHandler( new UncaughtExceptionHandler()
        {
            @Override
            public void uncaughtException( final Thread t, final Throwable e )
            {
                e.printStackTrace();
                Assert.fail( t.getName() + ": " + e.getMessage() );
            }
        } );
        return t;
    }

    protected Map<String, Long> testTimings( final long startDelay, final TimedTask... tasks )
    {
        return testTimings( startDelay, Arrays.asList( tasks ) );
    }

    protected Map<String, Long> testTimings( final TimedTask... tasks )
    {
        return testTimings( 2, Arrays.asList( tasks ) );
    }

    protected Map<String, Long> testTimings( final List<TimedTask> tasks )
    {
        return testTimings( 2, tasks );
    }

    protected Map<String, Long> testTimings( final long startDelay, final List<TimedTask> tasks )
    {
        final CountDownLatch latch = new CountDownLatch( tasks.size() );
        for ( final TimedTask task : tasks )
        {
            task.setLatch( latch );
            newThread( name.getMethodName() + "::" + task.getName(), task ).start();
            try
            {
                Thread.sleep( startDelay );
            }
            catch ( final InterruptedException e )
            {
                Assert.fail( "Interrupted!" );
            }
        }

        try
        {
            latch.await();
        }
        catch ( final InterruptedException e )
        {
            Assert.fail( "Interrupted!" );
        }

        final Map<String, Long> timings = new HashMap<>();
        for ( final TimedTask task : tasks )
        {
            timings.put( task.getName(), task.getTimestamp() );
        }

        return timings;
    }

    protected void startRead( final long initialDelay, final JoinableOutputStream stream, final CountDownLatch latch )
    {
        startRead( initialDelay, -1, -1, stream, latch );
    }

    protected void startRead( final long initialDelay, final long readDelay, final long closeDelay,
                              final JoinableOutputStream stream, final CountDownLatch latch )
    {
        newThread( "reader" + readers++, new AsyncFileReader( initialDelay, readDelay, closeDelay, stream, latch ) ).start();
    }

    protected void startTimedRawRead( final File file, final long initialDelay, final long readDelay,
                                      final long closeDelay, final CountDownLatch latch )
    {
        newThread( "reader" + readers++, new AsyncFileReader( initialDelay, readDelay, closeDelay, file, latch ) ).start();
    }

    protected JoinableOutputStream startTimedWrite( final long delay, final CountDownLatch latch )
        throws Exception
    {
        final File file = temp.newFile();
        return startTimedWrite( file, delay, latch );
    }

    protected JoinableOutputStream startTimedWrite( final File file, final long delay, final CountDownLatch latch )
        throws Exception
    {
        final JoinableOutputStream stream = new JoinableOutputStream( file );

        newThread( "writer" + writers++, new TimedFileWriter( stream, delay, latch ) ).start();

        return stream;
    }

}
