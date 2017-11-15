/**
 * Copyright (C) 2015 Red Hat, Inc.
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
    private final JoinableFileManager mgr = new JoinableFileManager();

    @Test
    public void bigFileWritePerformanceTest()
            throws Exception
    {
        final File f = temp.newFile( "bigfile.txt" );
        final File fm = temp.newFile( "bigfileMrg.txt" );
        String content = createBigFileContent();

        // JFM file io
        long start = System.currentTimeMillis();
        try (OutputStream out = mgr.openOutputStream( fm ))
        {
            out.write( content.getBytes() );
        }
        long end = System.currentTimeMillis();
        final long jfmFileWritingTime = end - start;

        // Normal java file io
        start = System.currentTimeMillis();
        try (FileOutputStream stream = new FileOutputStream( f ))
        {
            stream.write( content.getBytes() );
        }
        end = System.currentTimeMillis();
        final long javaFileWritingTime = end - start;

        System.out.println( String.format( "File size: JFM file: %dM; Java FO file: %dM", fm.length() / 1024 / 1024,
                                           f.length() / 1024 / 1024 ) );
        System.out.println(
                String.format( "File IO time comparison: JFM writing: %dms; Java FO writing: %dms", jfmFileWritingTime,
                               javaFileWritingTime ) );

        //TODO: Seems current JFM writing performance is slower about 3 times than normal java File IO when writing a 50m file
        if ( jfmFileWritingTime / javaFileWritingTime > 7 )
        {
            fail( String.format(
                    "JFM writing performance is 7 times slower than normal Java FO writing against a %dm file",
                    f.length() / 1024 / 1024 ) );
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
        System.out.println(
                String.format( "File IO time comparison: JFM reading: %dms; Java FO reading: %dms", jfmFileReadingTime,
                               javaFileReadingTime ) );

        //TODO: Seems current JFM reading performance is slower more than 10 times than normal java File IO when reading a 50m file
        if ( jfmFileReadingTime / javaFileReadingTime > 15 )
        {
            fail( String.format(
                    "JFM reading performance is 15 times slower than normal Java FO reading against a %dm file",
                    f.length() / 1024 / 1024 ) );
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
