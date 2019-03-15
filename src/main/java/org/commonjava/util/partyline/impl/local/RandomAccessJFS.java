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

import org.commonjava.cdi.util.weft.SignallingLock;
import org.commonjava.cdi.util.weft.SignallingLocker;
import org.commonjava.util.partyline.lock.UnlockStatus;
import org.commonjava.util.partyline.lock.global.GlobalLockManager;
import org.commonjava.util.partyline.spi.JoinableFile;
import org.commonjava.util.partyline.spi.JoinableFilesystem;
import org.commonjava.util.partyline.callback.StreamCallbacks;
import org.commonjava.util.partyline.lock.local.LocalLockOwner;

import java.io.File;
import java.io.IOException;

import static org.commonjava.util.partyline.lock.LockLevel.write;
import static org.commonjava.util.partyline.lock.LockLevel.read;

public class RandomAccessJFS
                implements JoinableFilesystem
{
    private final SignallingLocker lockManager = new SignallingLocker();

    private final GlobalLockManager globalLockManager;

    public RandomAccessJFS()
    {
        this( null );
    }

    public RandomAccessJFS( final GlobalLockManager globalLockManager )
    {
        this.globalLockManager = globalLockManager;
    }

    @Override
    public JoinableFile getFile( final File file, final LocalLockOwner lockOwner, final StreamCallbacks callbacks,
                                 final boolean doOutput, SignallingLock opLock ) throws IOException
    {
        if ( globalLockManager != null )
        {
            boolean locked = globalLockManager.tryLock( file.getAbsolutePath(), doOutput ? write : read, -1 );
            if ( !locked )
            {
                throw new IOException( "File locked by others, path: " + file.getAbsolutePath() );
            }
        }
        return new RandomAccessJF( file, lockOwner, callbacks, doOutput, opLock, globalLockManager );
    }

    @Override
    public SignallingLocker getLocalLockManager()
    {
        return lockManager;
    }

    @Override
    public void updateDominantLocks( String path, UnlockStatus unlockStatus )
    {
    }
}
