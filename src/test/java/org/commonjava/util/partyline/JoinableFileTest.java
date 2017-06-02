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

import ch.qos.logback.core.util.FileSize;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.commonjava.util.partyline.fixture.TimedFileWriter;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.commonjava.util.partyline.LockLevel.read;
import static org.commonjava.util.partyline.LockLevel.write;
import static org.commonjava.util.partyline.fixture.ThreadDumper.timeoutRule;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

public class JoinableFileTest
    extends AbstractJointedIOTest
{

    @Rule
    public TestRule timeout = timeoutRule( 30, TimeUnit.SECONDS );

    @Test
    public void readExistingFile()
            throws Exception
    {
        File f = temp.newFile( "read-target.txt" );
        String src = "This is a test";
        FileUtils.write( f, src );

        final JoinableFile stream = new JoinableFile( f, newLockOwner( f.getAbsolutePath(), read ), false );

        System.out.println( "File length: " + f.length() );

        String result = FileUtils.readFileToString( f );
        assertThat( result, equalTo( src ) );
    }

    @Test
    public void readExisting1MbFile()
            throws Exception
    {
        assertReadOfExistingFileOfSize("1mb");
    }

    @Test
    public void readExisting11MbFile()
            throws Exception
    {
        assertReadOfExistingFileOfSize("11mb");
    }

    private LockOwner newLockOwner( String path, LockLevel level )
    {
        return new LockOwner( path, name.getMethodName(), level );
    }

    private void assertReadOfExistingFileOfSize( String s )
            throws IOException, InterruptedException
    {
        File f = temp.newFile( "read-target.txt" );
        int sz = (int) FileSize.valueOf( "11mb" ).getSize();
        byte[] src = new byte[sz];

        Random rand = new Random();
        rand.nextBytes( src );

        try (OutputStream out = new FileOutputStream( f ))
        {
            IOUtils.write( src, out );
        }

        final JoinableFile jf = new JoinableFile( f, newLockOwner( f.getAbsolutePath(), read ), false );

        try(InputStream stream = jf.joinStream())
        {
            byte[] result = IOUtils.toByteArray( stream );
            assertThat( result, equalTo( src ) );
        }
    }

    @Test
    public void lockDirectory()
            throws IOException
    {
        File dir = temp.newFolder();
        dir.mkdirs();
        JoinableFile jf = new JoinableFile( dir, newLockOwner( dir.getAbsolutePath(), read ), false );

        assertThat( jf.isWriteLocked(), equalTo( true ) );

        jf.close();
    }

    @Test
    public void lockDirectoryCannotBeWritten()
            throws IOException
    {
        File dir = temp.newFolder();
        dir.mkdirs();
        JoinableFile jf = new JoinableFile( dir, newLockOwner( dir.getAbsolutePath(), read ), false );

        assertThat( jf.isWriteLocked(), equalTo( true ) );

        OutputStream out = jf.getOutputStream();
        assertThat( out, nullValue() );

        jf.close();
    }

    @Test
    public void lockDirectoryIsNotJoinable()
            throws IOException
    {
        File dir = temp.newFolder();
        dir.mkdirs();
        JoinableFile jf = new JoinableFile( dir, newLockOwner( dir.getAbsolutePath(), read ), false );

        assertThat( jf.isWriteLocked(), equalTo( true ) );
        assertThat( jf.isJoinable(), equalTo( false ) );

        jf.close();
    }

    @Test( expected = IOException.class )
    public void lockDirectoryJoinFails()
            throws IOException, InterruptedException
    {
        File dir = temp.newFolder();
        dir.mkdirs();
        JoinableFile jf = new JoinableFile( dir, newLockOwner( dir.getAbsolutePath(), read ), false );

        assertThat( jf.isWriteLocked(), equalTo( true ) );

        jf.joinStream();
    }

    @Test
    public void writeToFile()
        throws Exception
    {
        final ExecutorService execs = Executors.newFixedThreadPool( 1 );
        final CountDownLatch latch = new CountDownLatch( 1 );

        final File tempFile = temp.newFile();
        String threadName = "writer" + writers++;

        final JoinableFile stream =
                new JoinableFile( tempFile, new LockOwner( tempFile.getAbsolutePath(), name.getMethodName(), LockLevel.write ), true );

        execs.execute( () -> {
            Thread.currentThread().setName( threadName );
            new TimedFileWriter( stream, 0, latch ).run();
        } );

        System.out.println( "Waiting for " + name.getMethodName() + " threads to complete." );
        latch.await();

        final File file = new File( stream.getPath() );

        System.out.println( "File length: " + file.length() );

        final List<String> lines = FileUtils.readLines( file );
        System.out.println( lines );

        assertThat( lines.size(), equalTo( COUNT ) );
    }

    @Test
    public void overwriteFile_SmallerReplacementTruncates()
            throws Exception
    {
        File f = temp.newFile();
        JoinableFile jf = new JoinableFile( f, newLockOwner( f.getAbsolutePath(), write ), true );
        OutputStream stream = jf.getOutputStream();

        String longer = "This is a really really really long string";
        stream.write( longer.getBytes() );
        stream.close();

        jf = new JoinableFile( f, newLockOwner( f.getAbsolutePath(), write ), true );
        stream = jf.getOutputStream();

        String shorter = "This is a short string";
        stream.write( shorter.getBytes() );
        stream.close();

        final File file = new File( jf.getPath() );

        System.out.println( "File length: " + file.length() );
        assertThat( file.length(), equalTo( (long) shorter.getBytes().length ) );

        String content = FileUtils.readFileToString( f );
        assertThat( content, equalTo( shorter ) );
    }

    @Test
    public void joinFileRead()
            throws Exception
    {
        final File file = temp.newFile();
        final File f = temp.newFile();
        String str = "This is a test";
        FileUtils.write( f, str );
        JoinableFile jf = null;
        InputStream s1 = null;
        InputStream s2 = null;

        Logger logger = LoggerFactory.getLogger( getClass() );
        try
        {
            jf = new JoinableFile( f, newLockOwner( f.getAbsolutePath(), read ), false );
            s1 = jf.joinStream();
            s2 = jf.joinStream();

            logger.info( "READ first thread" );
            String out1 = IOUtils.toString( s1 );
            logger.info( "READ second thread" );
            String out2 = IOUtils.toString( s2 );

            assertThat( "first reader returned wrong data", out1, equalTo( str ) );
            assertThat( "second reader returned wrong data", out2, equalTo( str ) );
        }
        finally
        {
            logger.info( "CLOSE first thread" );
            IOUtils.closeQuietly( s1 );
            logger.info( "CLOSE second thread" );
            IOUtils.closeQuietly( s2 );
        }

        assertThat( jf, notNullValue() );
        assertThat( jf.isOpen(), equalTo( false ) );
    }

    @Test
    public void joinFileWriteContinueAfterInputStreamClose()
            throws Exception
    {
        final ExecutorService execs = Executors.newFixedThreadPool( 1 );
        final CountDownLatch latch = new CountDownLatch( 1 );

        final File tempFile = temp.newFile();
        String threadName = "writer" + writers++;

        final JoinableFile stream =
                new JoinableFile( tempFile, new LockOwner( tempFile.getAbsolutePath(), name.getMethodName(), LockLevel.write ), true );

        execs.execute( () -> {
            Thread.currentThread().setName( threadName );
            new TimedFileWriter( stream, 1, latch ).run();
        } );

        InputStream inStream = stream.joinStream();
        InputStream inStream2 = stream.joinStream();
        Thread.sleep(1000);
        inStream.close();
        inStream2.close();
        System.out.println( "All input stream closed. Waiting for " + name.getMethodName() + " writer thread to complete." );
        latch.await();

        final File file = new File( stream.getPath() );
        System.out.println( "File length: " + file.length() );

        final List<String> lines = FileUtils.readLines( file );
        System.out.println( lines );

        assertThat( lines.size(), equalTo( COUNT ) );
    }
}
