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
import org.apache.commons.io.IOUtils;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;

import static org.commonjava.util.partyline.fixture.ThreadDumper.timeoutRule;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

public class JoinableFileManagerTest
    extends AbstractJointedIOTest
{

    private static final long SHORT_TIMEOUT = 10;

    @Rule
    public TestRule timeout = timeoutRule( 30, TimeUnit.SECONDS );

    private final JoinableFileManager mgr = new JoinableFileManager();

    @Test
    public void lockWriteDoesntPreventOpenInputStream()
            throws Exception
    {
        String src = "This is a test";

        File f = temp.newFile();
        FileUtils.write( f, src );

        assertThat( "Write lock failed.", mgr.lock( f, -1, LockLevel.write, "test" ), equalTo( true ) );

        InputStream stream = mgr.openInputStream( f );
        assertThat( "InputStream cannot be null", stream, notNullValue() );

        String result = IOUtils.toString( stream );
        assertThat( result, equalTo( src ) );
    }

    @Test
    public void waitForLockThenOpenOutputStream()
            throws Exception
    {
        final File f = temp.newFile();
        assertThat( mgr.waitForWriteUnlock( f ), equalTo( true ) );

        try(OutputStream stream = mgr.openOutputStream( f ))
        {
            //nop
        }

        assertThat( mgr.isWriteLocked( f ), equalTo( false ) );
    }

    @Test
    public void openOutputStream_VerifyWriteLocked_NotReadLocked()
        throws Exception
    {
        final File f = temp.newFile();
        final OutputStream stream = mgr.openOutputStream( f );

        assertThat( mgr.isWriteLocked( f ), equalTo( true ) );
        assertThat( mgr.isReadLocked( f ), equalTo( false ) );

        stream.close();

        assertThat( mgr.isWriteLocked( f ), equalTo( false ) );
        assertThat( mgr.isReadLocked( f ), equalTo( false ) );
    }

    @Test( expected = IOException.class )
    public void openOutputStream_TimeBoxedSecondCallThrowsException()
            throws Exception
    {
        final File f = temp.newFile();
        Thread.currentThread().setName( "output 1" );
        final OutputStream stream = mgr.openOutputStream( f );

        Thread.currentThread().setName( "output 2" );
        OutputStream s2 = mgr.openOutputStream( f, SHORT_TIMEOUT );

        assertThat( s2, nullValue() );

        Thread.currentThread().setName( "output 1" );
        stream.close();

        Thread.currentThread().setName( "output 3" );
        s2 = mgr.openOutputStream( f, SHORT_TIMEOUT );

        assertThat( s2, notNullValue() );
    }

    @Test
    public void openInputStream_VerifyWriteLocked_ReadLocked()
        throws Exception
    {
        final File f = temp.newFile();
        final InputStream stream = mgr.openInputStream( f );

        assertThat( mgr.isWriteLocked( f ), equalTo( true ) );

        // after change to always use JoinableFile (even for read-first), I don't think partyline will ever read lock.
        assertThat( mgr.isReadLocked( f ), equalTo( false ) );

        stream.close();

        assertThat( mgr.isWriteLocked( f ), equalTo( false ) );
        assertThat( mgr.isReadLocked( f ), equalTo( false ) );
    }

    @Test
    @Ignore( "With change to JoinableFile for all reads, partyline should never read-lock" )
    public void openInputStream_TimeBoxedSecondCallReturnsNull()
        throws Exception
    {
        final File f = temp.newFile();
        final InputStream stream = mgr.openInputStream( f );

        InputStream s2 = mgr.openInputStream( f, SHORT_TIMEOUT );

        assertThat( s2, nullValue() );

        stream.close();

        s2 = mgr.openInputStream( f, SHORT_TIMEOUT );

        assertThat( s2, notNullValue() );
    }

    @Test
    public void openInputStream_cleanupCurrentThread_openOutputStream()
        throws Exception
    {
        final File f = temp.newFile("test.txt");
        FileUtils.write( f, "This is first pass" );
        mgr.openInputStream( f );
        mgr.cleanupCurrentThread();
        OutputStream outputStream = mgr.openOutputStream( f );

        outputStream.close();
    }

}
