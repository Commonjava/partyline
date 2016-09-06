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

import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.fail;

/**
 * Author: Michal Szynkiewicz, michal.l.szynkiewicz@gmail.com
 * Date: 9/3/16
 * Time: 10:46 PM
 */
public class BinaryFileTest {

    private static File binaryFile;

    @BeforeClass
    public static void prepareBinaryFile() throws IOException
    {
        binaryFile = File.createTempFile( "BinaryFileTest", ".bin" );
        try (FileOutputStream fos = new FileOutputStream(binaryFile))
        {
            for ( int i = 0; i < 255; i++ )
            {
                fos.write(i);
            }
        }
        binaryFile.deleteOnExit();
    }

    @Test
    public void shouldReadBinaryFile() throws IOException
    {
        List<String> failures = new ArrayList<>();

        FileInputStream expected = new FileInputStream( binaryFile );
        InputStream actual = new JoinableFileManager().openInputStream( binaryFile );

        int pos = 0;
        int exp, act;

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
