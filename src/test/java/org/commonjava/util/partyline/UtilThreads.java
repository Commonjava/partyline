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

import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.CountDownLatch;

/**
 * Created by jdcasey on 12/8/16.
 */
public final class UtilThreads
{
    private UtilThreads(){}

    public static Runnable writer( JoinableFileManager manager, File f, CountDownLatch masterLatch )
    {
        return writer( manager, f, masterLatch, null );
    }

    public static Runnable writer( JoinableFileManager manager, File f, CountDownLatch masterLatch, CountDownLatch readEndLatch )
    {
        return () -> {
            Thread.currentThread().setName( "openOutputStream" );
            System.out.println("Starting: " + Thread.currentThread().getName());

            if ( readEndLatch != null )
            {
                try
                {
                    System.out.println("awaiting read-end latch");
                    readEndLatch.await();
                }
                catch ( InterruptedException e )
                {
                    return;
                }
            }

            try (OutputStream o = manager.openOutputStream( f ))
            {
                o.write( "Test data".getBytes() );
            }
            catch ( Exception e )
            {
                e.printStackTrace();
            }
            System.out.println("Counting down master latch");
            masterLatch.countDown();
            System.out.println(
                    String.format( "[%s] Count down after write thread: %s", Thread.currentThread().getName(),
                                   masterLatch.getCount() ) );
        };
    }

    public static Runnable reader( int k, JoinableFileManager manager, File f, CountDownLatch masterLatch )
    {
        return reader( k, manager, f, masterLatch, null, null, null );
    }

    public static Runnable reader( int k, JoinableFileManager manager, File f, CountDownLatch masterLatch, boolean binaryContent )
    {
        return reader( k, manager, f, masterLatch, null, null, null, binaryContent );
    }

    public static Runnable reader( int k, JoinableFileManager manager, File f, CountDownLatch masterLatch, CountDownLatch readBeginLatch, CountDownLatch readEndLatch, CountDownLatch deleteEndLatch )
    {
        return reader( k, manager, f, masterLatch, readBeginLatch, readEndLatch, deleteEndLatch, true );
    }

    public static Runnable reader( int k, JoinableFileManager manager, File f, CountDownLatch masterLatch, CountDownLatch readBeginLatch, CountDownLatch readEndLatch, CountDownLatch deleteEndLatch, boolean binaryContent )
    {
        return () -> {
            Thread.currentThread().setName( "openInputStream-" + k );
            System.out.println("Starting: " + Thread.currentThread().getName());

            if ( deleteEndLatch != null )
            {
                try
                {
                    System.out.println("awaiting delete latch");
                    deleteEndLatch.await();
                }
                catch ( InterruptedException e )
                {
                    return;
                }
            }

            if ( readBeginLatch != null )
            {
                System.out.println("Counting down read-begin latch");
                readBeginLatch.countDown();
                try
                {
                    System.out.println("awaiting read-begin latch");
                    readBeginLatch.await();
                }
                catch ( InterruptedException e )
                {
                    return;
                }
            }

            try (InputStream s = manager.openInputStream( f ))
            {
                if ( binaryContent )
                {
                    byte[] data = IOUtils.toByteArray( s );
                    System.out.println( Thread.currentThread().getName() + ": Read " + data.length + " bytes." );
                }
                else
                {
                    System.out.println( Thread.currentThread().getName() + ": " + IOUtils.toString( s ) );
                }
            }
            catch ( Exception e )
            {
                e.printStackTrace();
            }
            finally
            {
                if ( readEndLatch != null )
                {
                    System.out.println("Counting down read-end latch");
                    readEndLatch.countDown();
                }
                System.out.println("Counting down master latch");
                masterLatch.countDown();
                System.out.println(
                        String.format( "[%s] Count down after %s read thread: %s", Thread.currentThread().getName(), k,
                                       masterLatch.getCount() ) );
            }
        };
    }

    public static Runnable deleter( JoinableFileManager manager, File f, CountDownLatch masterLatch )
    {
        return deleter( manager, f, masterLatch, null );
    }

    public static Runnable deleter( JoinableFileManager manager, File f, CountDownLatch masterLatch, CountDownLatch deleteEndLatch )
    {
        return () -> {
            Thread.currentThread().setName( "delete" );
            System.out.println("Starting: " + Thread.currentThread().getName());
            try
            {
                manager.tryDelete( f );
            }
            catch ( Exception e )
            {
                e.printStackTrace();
            }
            finally
            {
                if ( deleteEndLatch != null )
                {
                    System.out.println("Counting down delete latch");
                    deleteEndLatch.countDown();
                }
                System.out.println("Counting down master latch");
                masterLatch.countDown();
                System.out.println(
                        String.format( "[%s] Count down after delete thread: %s", Thread.currentThread().getName(),
                                       masterLatch.getCount() ) );
            }
        };
    }
}
