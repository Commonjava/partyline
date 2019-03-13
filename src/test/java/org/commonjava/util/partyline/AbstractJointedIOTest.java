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

import org.commonjava.util.partyline.lock.global.GlobalLockOwner;
import org.commonjava.util.partyline.lock.global.impl.InfinispanTransactionalGLM;
import org.infinispan.Cache;
import org.infinispan.manager.DefaultCacheManager;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import java.io.File;
import java.io.IOException;

public abstract class AbstractJointedIOTest
{
    private final Logger logger = LoggerFactory.getLogger( getClass() );

    public static final int COUNT = 2000;

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    @Rule
    public TestName name = new TestName();

    private final SignallingLocker<String> locker = new SignallingLocker<>();


    protected int readers = 0;

    protected int writers = 0;

    protected DefaultCacheManager manager;

    protected boolean isGlobalTest()
    {
        return true;
    }

    protected Partyline getPartylineInstance()
    {
        if ( isGlobalTest() )
        {
            // by default we run global test
            try
            {
                manager = new DefaultCacheManager( "infinispan.xml" );
            }
            catch ( IOException e )
            {
                logger.error( "Load infinispan.xml failed, use 'new Partyline()'", e );
                return null;
            }

            Cache<String, GlobalLockOwner> partylineLocksCache = manager.getCache( "partylineLocks" );

            logger.debug( "With partylineLocksCache, txManager: {}",
                          partylineLocksCache.getAdvancedCache().getTransactionManager() );
            return new Partyline( new InfinispanTransactionalGLM( partylineLocksCache ) );
        }
        else
        {
            return new Partyline();
        }
    }

    protected void stopCacheManager()
    {
        manager.stop();
    }
}
