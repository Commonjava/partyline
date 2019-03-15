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

import org.junit.ClassRule;
import org.junit.rules.TestRule;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.commonjava.util.partyline.fixture.ThreadDumper.timeoutRule;

/**
 * Created by jdcasey on 12/8/16.
 */
public class AbstractBytemanTest
        extends AbstractJointedIOTest
{

    @ClassRule
    public static TestRule timeout = timeoutRule( 30, TimeUnit.SECONDS );

    protected String raceExecutions( final Map<String,Runnable> executions )
            throws InterruptedException
    {
        final ExecutorService execs = Executors.newFixedThreadPool( executions.size() );
        final CountDownLatch latch = new CountDownLatch( executions.size() );

        List<String> result = new ArrayList<>();

        executions.forEach( (label, runnable)-> execs.execute( ()->{
            try
            {
                runnable.run();
                synchronized ( result )
                {
                    result.add( label );
                }
            }
            finally
            {
                System.out.println("Finished: " + label);
                latch.countDown();
            }
        } ) );

        latch.await();

        return result.get( 0 );
    }

}
