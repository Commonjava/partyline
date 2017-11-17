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
import org.commonjava.cdi.util.weft.ThreadContext;
import org.junit.Test;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.Map;

import static org.commonjava.util.partyline.JoinableFileManager.PARTYLINE_OPEN_FILES;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class ClearThreadContextClosesMappedOpenStreamsTest
                extends AbstractJointedIOTest
{

    @Test
    public void run() throws Exception
    {
        final JoinableFileManager manager = new JoinableFileManager();
        ThreadContext context = ThreadContext.getContext( true );
        final File f = temp.newFile( "test.txt" );
        FileUtils.write( f, "This is a test" );

        try
        {
            manager.openOutputStream( f );
            Map<String, WeakReference<Closeable>> open1 =
                            (Map<String, WeakReference<Closeable>>) context.get( PARTYLINE_OPEN_FILES );
            assertThat( open1.size(), equalTo( 1 ) );

            ThreadContext.clearContext();
            Map<String, WeakReference<Closeable>> open2 =
                            (Map<String, WeakReference<Closeable>>) context.get( PARTYLINE_OPEN_FILES );
            assertThat( open2, equalTo( null ) );
        }
        catch ( IOException e )
        {
            e.printStackTrace();
            fail( "Failed to outputStream io: " + e.getMessage() );
        }
    }
}
