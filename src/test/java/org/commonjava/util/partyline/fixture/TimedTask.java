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
/*******************************************************************************
* Copyright (c) 2015 Red Hat, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the GNU Public License v3.0
* which accompanies this distribution, and is available at
* http://www.gnu.org/licenses/gpl.html
*
* Contributors:
* Red Hat, Inc. - initial API and implementation
******************************************************************************/
package org.commonjava.util.partyline.fixture;

import java.util.concurrent.CountDownLatch;

public class TimedTask
    implements Runnable
{
    private final String name;

    private long timestamp;

    private final Runnable runnable;

    private CountDownLatch latch;

    public TimedTask( final String name, final Runnable runnable )
    {
        this.name = name;
        this.runnable = runnable;
    }

    @Override
    public void run()
    {
        runnable.run();
        timestamp = System.nanoTime();
        if ( latch != null )
        {
            latch.countDown();
        }
    }

    public long getTimestamp()
    {
        return timestamp;
    }

    public String getName()
    {
        return name;
    }

    public void setLatch( final CountDownLatch latch )
    {
        this.latch = latch;
    }

}
