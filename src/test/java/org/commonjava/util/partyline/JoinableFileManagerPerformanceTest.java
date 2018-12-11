/**
 * Copyright (C) 2015 Red Hat, Inc. (nos-devel@redhat.com)
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

import org.commonjava.util.partyline.impl.local.RandomAccessJFS;
import org.commonjava.util.partyline.lock.LockLevel;
import org.commonjava.util.partyline.lock.local.LocalLockOwner;
import org.commonjava.util.partyline.lock.local.ReentrantOperationLock;
import org.commonjava.util.partyline.spi.JoinableFile;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class JoinableFileManagerPerformanceTest
        extends AbstractJointedIOTest
{
    private final Partyline mgr = new Partyline();

    @Test
    public void bigFileWritePerformanceTest()
            throws Exception
    {
        final File f = temp.newFile( "bigfile.txt" );
        final File fm = temp.newFile( "bigfileJFM.txt" );
        final File jft = temp.newFile( "bigfileJF.txt" );

        String content = createBigFileContent();

        JoinableFile jf =
                new RandomAccessJFS().getFile( jft, new LocalLockOwner( jft.getPath(), "write test", LockLevel.write ),
                                               null, true, new ReentrantOperationLock() );
        long start = System.currentTimeMillis();
        System.out.println("Opening output...");
        try(OutputStream out = jf.getOutputStream())
        {
            System.out.println("Writing content");
            out.write( content.getBytes() );
            System.out.println("closing stream");

        }
        long end = System.currentTimeMillis();
        System.out.println("closing file");
        jf.close();
        System.out.println("done");
        long jfTime = end-start;

        // JFM file io
        start = System.currentTimeMillis();
        try (OutputStream out = mgr.openOutputStream( fm ))
        {
            out.write( content.getBytes() );
        }
        end = System.currentTimeMillis();
        final long jfmFileWritingTime = end - start;

        // Normal java file io
        start = System.currentTimeMillis();
        try (FileOutputStream stream = new FileOutputStream( f ))
        {
            stream.write( content.getBytes() );
        }
        end = System.currentTimeMillis();
        final long javaFileWritingTime = end - start;

        System.out.println( String.format( "File size: JFM file: %dM; JF file: %dM; Java FO file: %dM", fm.length() / 1024 / 1024, jft.length() / 1024 / 1024,
                                           f.length() / 1024 / 1024 ) );

        System.out.printf( "File IO time comparison: JFM writing: %dms; JF writing: %dms; Java FO writing: %dms. Ratio JFM/FO: %d, JF/FO: %d\n", jfmFileWritingTime,
                           jfTime, javaFileWritingTime, (jfmFileWritingTime/javaFileWritingTime), (jfTime/javaFileWritingTime) );

        //TODO: Seems current JFM writing performance is slower about 3 times than normal java File IO when writing a 50m file
        final int times = 10;
        if ( jfmFileWritingTime / javaFileWritingTime > times )
        {
            fail( String.format(
                    "JFM writing performance is %d times slower than normal Java FO writing against a %dm file",
                    (jfmFileWritingTime/javaFileWritingTime), f.length() / 1024 / 1024 ) );
        }
    }

    @Test
    public void bigFileReadPerformanceTest()
            throws Exception
    {
        final File f = temp.newFile( "bigfile-read.txt" );
        String content = createBigFileContent();
        final int contentSize = content.getBytes().length;
        try (FileOutputStream stream = new FileOutputStream( f ))
        {
            stream.write( content.getBytes() );
        }

        // JFM file io
        long start = System.currentTimeMillis();
        try (InputStream in = mgr.openInputStream( f ))
        {
            byte[] contentBytes = new byte[( contentSize )];
            in.read( contentBytes );
            assertThat( new String( contentBytes ), equalTo( content ) );
        }
        long end = System.currentTimeMillis();
        final long jfmFileReadingTime = end - start;

        // Java FI io
        start = System.currentTimeMillis();
        try (FileInputStream in = new FileInputStream( f ))
        {
            byte[] contentBytes = new byte[( contentSize )];
            in.read( contentBytes );
            assertThat( new String( contentBytes ), equalTo( content ) );
        }
        end = System.currentTimeMillis();
        final long javaFileReadingTime = end - start;

        System.out.println( String.format( "File size is: %dM", f.length() / 1024 / 1024 ) );
        System.out.printf( "File IO time comparison: JFM reading: %dms; Java FO reading: %dms. Ratio: %d\n", jfmFileReadingTime,
                               javaFileReadingTime, (jfmFileReadingTime/javaFileReadingTime) );

        //TODO: Seems current JFM reading performance is slower more than 10 times than normal java File IO when reading a 50m file
        final int times = 15;
        if ( jfmFileReadingTime / javaFileReadingTime > times )
        {
            fail( String.format(
                    "JFM reading performance is %d times slower than normal Java FO reading against a %dm file",
                    (jfmFileReadingTime/javaFileReadingTime), f.length() / 1024 / 1024 ) );
        }
    }

    private String createBigFileContent()
    {
        // File content about 50m
        final int loop = 1024 * 1024;
        StringBuilder builder = new StringBuilder();
        for ( int i = 0; i < loop; i++ )
        {
            builder.append( "This is a big file, will concat many many times." );
        }
        return builder.toString();
    }

}
