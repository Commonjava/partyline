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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.io.IOUtils;

public final class AsyncFileReader
    implements Runnable
{

    private final JoinableFile stream;

    private final CountDownLatch latch;

    private final long initialDelay;

    private final long closeDelay;

    private final File file;

    private final long readDelay;

    public AsyncFileReader( final long initialDelay, final long readDelay, final long closeDelay,
                            final JoinableFile stream,
                            final CountDownLatch latch )
    {
        this.initialDelay = initialDelay;
        this.readDelay = readDelay;
        this.closeDelay = closeDelay;
        this.stream = stream;
        this.latch = latch;
        this.file = null;
    }

    public AsyncFileReader( final long initialDelay, final long readDelay, final long closeDelay, final File file,
                            final CountDownLatch latch )
    {
        this.initialDelay = initialDelay;
        this.readDelay = readDelay;
        this.closeDelay = closeDelay;
        this.file = file;
        this.latch = latch;
        this.stream = null;
    }

    @Override
    public void run()
    {
        System.out.println( Thread.currentThread()
                                  .getName() + ": Waiting " + initialDelay + "ms to initialize read." );
        try
        {
            Thread.sleep( initialDelay );
        }
        catch ( final InterruptedException e )
        {
            return;
        }

        InputStream inStream = null;
        try
        {
            if ( stream != null )
            {
                inStream = stream.joinStream();
            }
            else
            {
                inStream = new FileInputStream( file );
            }
            System.out.println( Thread.currentThread()
                                      .getName() + ": Opened read stream" );

            if ( readDelay > -1 )
            {
                System.out.println( Thread.currentThread()
                                          .getName() + ": Reading with " + readDelay + "ms between each byte." );

                final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                int read = -1;
                while ( ( read = inStream.read() ) > -1 )
                {
                    baos.write( read );
                    Thread.sleep( readDelay );
                }

                System.out.println( baos );
            }
            else if ( closeDelay > -1 )
            {
                System.out.println( Thread.currentThread()
                                          .getName() + ": Reading with total delay of " + closeDelay + "ms." );
                final long end = System.currentTimeMillis() + closeDelay;
                while ( inStream.read() > -1 && System.currentTimeMillis() < end )
                {
                    Thread.sleep( 1 );
                }
            }
            else
            {
                final List<String> lines = IOUtils.readLines( inStream );

                System.out.println( Thread.currentThread()
                                          .getName() + ": " + lines );

                assertThat( Thread.currentThread()
                                  .getName() + " wrong number of lines read! Expected: " + AbstractJointedIOTest.COUNT + ", got: "
                    + lines.size(), lines.size(), equalTo( AbstractJointedIOTest.COUNT ) );
            }

            System.out.println( Thread.currentThread()
                                      .getName() + ": Read done." );
        }
        catch ( final Exception e )
        {
            e.printStackTrace();
        }
        finally
        {
            IOUtils.closeQuietly( inStream );
            latch.countDown();
        }
    }

}