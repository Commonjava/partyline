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
package org.commonjava.util.partyline.lock.global;

import org.commonjava.util.partyline.lock.LockLevel;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashSet;
import java.util.Set;

public class GlobalLockOwner
                implements Externalizable
{
    LockLevel level;

    public GlobalLockOwner( LockLevel level )
    {
        this.level = level;
    }

    Set<String> ownerSet = new HashSet<>();

    public GlobalLockOwner withOwner( String id )
    {
        ownerSet.add( id );
        return this;
    }

    public LockLevel getLevel()
    {
        return level;
    }

    public boolean containsOwner( String id )
    {
        return ownerSet.contains( id );
    }

    public void removeOwner( String id )
    {
        ownerSet.remove( id );
    }

    public boolean isEmpty()
    {
        return ownerSet.isEmpty();
    }

    @Override
    public void writeExternal( ObjectOutput objectOutput ) throws IOException
    {
        objectOutput.writeObject( level.name() );
        objectOutput.writeObject( ownerSet );
    }

    @Override
    public void readExternal( ObjectInput objectInput ) throws IOException, ClassNotFoundException
    {
        level = LockLevel.valueOf( (String) objectInput.readObject() );
        ownerSet = (Set) objectInput.readObject();
    }

    @Override
    public String toString()
    {
        return "GlobalLockOwner{" + "level=" + level + ", ownerSet=" + ownerSet + '}';
    }
}
