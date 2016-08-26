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

import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.junit.Assert.assertThat;

/**
 * Created by jdcasey on 8/18/16.
 */
public class FileTreeTest
{

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void addChildAndVerifyHasChildren()
            throws IOException
    {
        FileTree root = new FileTree();
        File child = createStructure( "child.txt", true );
        JoinableFile jf = new JoinableFile( child, false );
        root.add( jf );

        assertThat( root.hasChildren( child.getParentFile() ), equalTo( true ) );
    }

    @Test
    public void addDirAndVerifyAncestorOfChild()
            throws IOException
    {
        FileTree root = new FileTree();
        File child = createStructure( "parent/child.txt", true );
        File parent = child.getParentFile();
        JoinableFile jf = new JoinableFile( parent, false );
        root.add( jf );

        JoinableFile result = root.findAncestorFile( child, (file)->true );

        assertThat( result, notNullValue() );
        assertThat( result, sameInstance( jf ) );
    }

    @Test
    public void addChildAndRetrieve()
            throws IOException
    {
        FileTree root = new FileTree();
        File child = createStructure( "child.txt", true );
        JoinableFile jf = new JoinableFile( child, false );
        root.add( jf );

        JoinableFile result = root.getFile( child );

        assertThat( result, notNullValue() );
    }

    @Test
    public void addChildAndRenderTree()
            throws IOException
    {
        FileTree root = new FileTree();
        File child = createStructure( "child.txt", true );
        JoinableFile jf = new JoinableFile( child, false );
        root.add( jf );

        System.out.println(root.renderTree());
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
