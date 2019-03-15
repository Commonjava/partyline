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
