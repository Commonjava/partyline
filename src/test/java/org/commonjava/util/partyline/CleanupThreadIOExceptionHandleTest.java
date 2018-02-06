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
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitConfig;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.Map;

import static org.commonjava.util.partyline.JoinableFileManager.PARTYLINE_OPEN_FILES;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

@RunWith( BMUnitRunner.class )
public class CleanupThreadIOExceptionHandleTest
                extends AbstractBytemanTest
{
    /**
     * Simulate an IOException throw when JoinableFileManager try to close an outputStream in the method cleanupCurrentThread.
     * Partyline will re-add the outputStream to the Thread Context if an io error occurred during its close process.
     * Avoiding Partyline's IO error will be the real solution of caller's stream's re-close issue.
     *
     * @throws Exception
     */

    @BMRule( name = "cleanupThreadIOExceptionHandleTest", targetClass = "java.io.OutputStream", targetMethod = "close", targetLocation = "ENTRY", action = "throw new java.io.IOException()" )
    @Test
    @BMUnitConfig( debug = true )
    @Ignore
    public void run() throws Exception
    {
        final JoinableFileManager manager = new JoinableFileManager();
        ThreadContext context = ThreadContext.getContext( true );
        final File f = temp.newFile( "test-byteman.txt" );
        FileUtils.write( f, "This is an IO Exception byteman test" );
        try
        {
            manager.openOutputStream( f );
            Map<String, WeakReference<Closeable>> open1 =
                            (Map<String, WeakReference<Closeable>>) context.get( PARTYLINE_OPEN_FILES );
            assertThat( open1.size(), equalTo( 1 ) );

            manager.cleanupCurrentThread();
            Map<String, WeakReference<Closeable>> open2 =
                            (Map<String, WeakReference<Closeable>>) context.get( PARTYLINE_OPEN_FILES );
            assertThat( open2.size(), equalTo( 1 ) );
        }
        catch ( IOException e )
        {
            e.printStackTrace();
            fail( "Failed to outputStream io: " + e.getMessage() );
        }
    }
}
