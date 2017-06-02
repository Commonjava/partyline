package org.commonjava.util.partyline;

import org.junit.Test;

import java.util.Optional;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Created by jdcasey on 6/2/17.
 */
public class LockOwnerTest
{

    @Test
    public void lockLevelSorting()
    {
        Optional<LockLevel> first = Stream.of( LockLevel.values() )
                                          .sorted( ( o1, o2 ) -> new Integer( o2.ordinal() ).compareTo( o1.ordinal() ) )
                                          .findFirst();

        assertThat( first.get(), equalTo( LockLevel.delete ) );
    }

    @Test
    public void lockClearAndLockAgainWithDifferentLevel()
    {
        LockOwner owner = new LockOwner( "/path/to/nowhere", "testing", LockLevel.write );

        assertThat( owner.isLockedByCurrentThread(), equalTo( true ) );

        boolean unlocked = owner.unlock();

        assertThat( unlocked, equalTo( true ) );
        assertThat( owner.isLockedByCurrentThread(), equalTo( false ) );

        boolean locked = owner.lock( "relocking", LockLevel.delete );
        assertThat( locked, equalTo( true ) );
    }
}
