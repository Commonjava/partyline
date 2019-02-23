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
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.Assert.fail;

/**
 * Author: Michal Szynkiewicz, michal.l.szynkiewicz@gmail.com
 * Date: 9/3/16
 * Time: 10:46 PM
 */
public class BinaryFileTest extends AbstractJointedIOTest {

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    @Rule
    public TestName name = new TestName();

    private void writeBinaryFile( OutputStream jos, ByteArrayOutputStream written ) throws IOException
    {
        try
        {
            for ( int i = 0; i < 255; i++ )
            {
                jos.write(i);
                written.write( i );
            }
        }
        finally
        {
            IOUtils.closeQuietly( jos );
        }
    }

    @Test
    public void shouldReadBinaryFile()
            throws IOException, InterruptedException
    {
        List<String> failures = new ArrayList<>();

        File binaryFile = temp.newFile( "binary-file.bin" );
        ReentrantLock lock = new ReentrantLock();
        JoinableFile jf = newFile( binaryFile, new LockOwner( binaryFile.getAbsolutePath(),
                                                                         name.getMethodName(), LockLevel.write ), true );
        OutputStream jos = jf.getOutputStream();
        InputStream actual = jf.joinStream();

        ByteArrayOutputStream written = new ByteArrayOutputStream();
        writeBinaryFile( jos, written );

        int pos = 0;
        int exp, act;

        ByteArrayInputStream expected = new ByteArrayInputStream( written.toByteArray() );
        while ( ( exp = expected.read() ) != -1 )
        {
            act = actual.read();
            pos++;

            if ( act != exp )
            {
                failures.add( String.format( "Failure at position %d. Expected %d, got %d", pos, exp, act ) );
            }
        }

        if ( !failures.isEmpty() )
        {
            fail( "Failures: " + failures );
        }
    }
}
