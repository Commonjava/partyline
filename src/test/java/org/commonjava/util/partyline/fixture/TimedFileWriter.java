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
package org.commonjava.util.partyline.fixture;

import java.util.concurrent.CountDownLatch;

import org.apache.commons.io.IOUtils;
import org.commonjava.util.partyline.AbstractJointedIOTest;
import org.commonjava.util.partyline.JoinableFile;

public final class TimedFileWriter
    implements Runnable
{
    private final long delay;

    private final JoinableFile stream;

    private final CountDownLatch latch;

    public TimedFileWriter( final JoinableFile stream, final long delay, final CountDownLatch latch )
    {
        this.stream = stream;
        this.delay = delay;
        this.latch = latch;
    }

    @Override
    public void run()
    {
        System.out.println( Thread.currentThread()
                                  .getName() + ": initializing write." );
        try
        {
            System.out.println( Thread.currentThread()
                                      .getName() + ": Writing with per-line delay of: " + delay );
            for ( int i = 0; i < AbstractJointedIOTest.COUNT; i++ )
            {
                //                    System.out.println( "Sleeping: " + delay );
                Thread.sleep( delay );

                //                    System.out.println( "Writing: " + i );
                stream.getOutputStream().write( String.format( "%d\n", i )
                                    .getBytes() );
            }

            System.out.println( Thread.currentThread()
                                      .getName() + ": write done." );
        }
        catch ( final Exception e )
        {
            e.printStackTrace();
        }
        finally
        {
            IOUtils.closeQuietly( stream );
            latch.countDown();
        }
    }
}