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

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Created by jdcasey on 8/18/16.
 */
public class FileTreeTest
{

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    @Rule
    public TestName name = new TestName();

    @Test
    public void lockDirThenLockFile()
            throws IOException, InterruptedException
    {
        FileTree root = new FileTree();
        String dirPath = "directory";
        File child = createStructure( Paths.get( dirPath, "child.txt" ).toString(), true );
        File dir = child.getParentFile();

        boolean dirLocked = root.tryLock( dir, "lock directory", LockLevel.write, 2000, TimeUnit.MILLISECONDS );

        assertThat( dirLocked, equalTo( true ) );

        try (JoinableFile jf = root.setOrJoinFile( child, null, true, 2000, TimeUnit.MILLISECONDS,
                                                   ( result ) -> result );
             OutputStream out = jf.getOutputStream())
        {
            IOUtils.write( "This is a test", out );
        }
    }

    @Test
    public void lockDirThenLockTwoFiles()
            throws IOException, InterruptedException
    {
        FileTree root = new FileTree();
        String dirPath = "directory";
        File child = createStructure( Paths.get( dirPath, "child.txt" ).toString(), true );
        File child2 = createStructure( Paths.get( dirPath, "child2.txt" ).toString(), true );
        File dir = child.getParentFile();

        boolean dirLocked = root.tryLock( dir, "lock directory", LockLevel.write, 2000, TimeUnit.MILLISECONDS );

        assertThat( dirLocked, equalTo( true ) );

        try (JoinableFile jf = root.setOrJoinFile( child, null, true, 2000, TimeUnit.MILLISECONDS,
                                                   ( result ) -> result );
             JoinableFile jf2 = root.setOrJoinFile( child2, null, true, 2000, TimeUnit.MILLISECONDS,
                                                    ( result ) -> result );
             OutputStream out = jf.getOutputStream();
             OutputStream out2 = jf2.getOutputStream();)
        {
            IOUtils.write( "This is a test", out2 );
            out2.close();

            IOUtils.write( "This is a test", out );
        }
    }

    @Test
    public void addChildAndRenderTree()
            throws IOException, InterruptedException
    {
        FileTree root = new FileTree();
        File child = createStructure( "child.txt", true );
        JoinableFile jf = root.setOrJoinFile( child, null, false, -1, TimeUnit.MILLISECONDS, ( result ) -> result );
        //        JoinableFile jf = new JoinableFile( child, false );
        //        root.add( jf );

        System.out.println( "File tree rendered as:\n" + root.renderTree() );
    }

    private File createStructure( String path, boolean writeTestFile )
            throws IOException
    {
        LinkedList<String> parts = new LinkedList<>( Arrays.asList( path.split( "/" ) ) );
        String fname = parts.removeLast();

        File current = temp.newFolder();
        while ( !parts.isEmpty() )
        {
            current = new File( current, parts.removeFirst() );
        }

        current = new File( current, fname );
        if ( writeTestFile )
        {
            current.getParentFile().mkdirs();
            FileUtils.write( current, "This is a test" );
        }
        else
        {
            current.mkdirs();
        }

        return current;
    }
}
