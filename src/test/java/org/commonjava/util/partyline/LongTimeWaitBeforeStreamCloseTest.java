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
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;

import java.io.File;
import java.util.concurrent.TimeUnit;

import static org.commonjava.util.partyline.fixture.ThreadDumper.timeoutRule;

public class LongTimeWaitBeforeStreamCloseTest
                extends AbstractJointedIOTest
{

    @Rule
    public TestRule timeout = timeoutRule( 2 * 60, TimeUnit.SECONDS );

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    @Test
    @Ignore
    public void run() throws Exception
    {
        final Partyline manager = getPartylineInstance();
        final File f = temp.newFile( "long-wait-before-close.txt" );
        FileUtils.write( f, "This is a long time wait test" );

        manager.openOutputStream( f );
        Thread.sleep( 60 * 1000 );
        manager.cleanupCurrentThread();
    }
}