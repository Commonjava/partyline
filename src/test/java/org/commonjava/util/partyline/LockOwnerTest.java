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
        String label = "testing";
        LockOwner owner = new LockOwner( "/path/to/nowhere", label, LockLevel.write );

        assertThat( owner.isLockedByCurrentThread(), equalTo( true ) );

        boolean unlocked = owner.unlock( label );

        assertThat( unlocked, equalTo( true ) );
        assertThat( owner.isLockedByCurrentThread(), equalTo( false ) );

        boolean locked = owner.lock( "relocking", LockLevel.delete );
        assertThat( locked, equalTo( true ) );
    }
}
