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

import java.util.concurrent.CountDownLatch;

import org.apache.commons.io.IOUtils;
import org.commonjava.util.partyline.JoinableOutputStream;

public final class TimedFileWriter
    implements Runnable
{
    private final long delay;

    private final JoinableOutputStream stream;

    private final CountDownLatch latch;

    public TimedFileWriter( final JoinableOutputStream stream, final long delay, final CountDownLatch latch )
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
                stream.write( String.format( "%d\n", i )
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