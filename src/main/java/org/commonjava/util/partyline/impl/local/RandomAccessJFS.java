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
package org.commonjava.util.partyline.impl.local;

import org.commonjava.util.partyline.lock.LockLevel;
import org.commonjava.util.partyline.lock.UnlockStatus;
import org.commonjava.util.partyline.spi.JoinableFile;
import org.commonjava.util.partyline.spi.JoinableFilesystem;
import org.commonjava.util.partyline.callback.StreamCallbacks;
import org.commonjava.util.partyline.lock.local.LocalLockManager;
import org.commonjava.util.partyline.lock.local.LocalLockOwner;
import org.commonjava.util.partyline.lock.local.ReentrantOperationLock;

import java.io.File;
import java.io.IOException;

public class RandomAccessJFS
        implements JoinableFilesystem
{
    private final LocalLockManager lockManager = new LocalLockManager();

    @Override
    public JoinableFile getFile( final File file, final LocalLockOwner lockOwner, final StreamCallbacks callbacks,
                                 final boolean doOutput, ReentrantOperationLock opLock )
            throws IOException
    {
        return new RandomAccessJF( file, lockOwner, callbacks, doOutput, opLock );
    }

    @Override
    public LocalLockManager getLocalLockManager()
    {
        return lockManager;
    }

    @Override
    public void updateDominantLocks( String path, UnlockStatus unlockStatus )
    {
    }
}
