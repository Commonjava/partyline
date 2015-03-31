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

import java.util.concurrent.Callable;

public class RunnableCallable
    implements Callable<Long>
{

    private final Runnable unit;

    public RunnableCallable( final Runnable unit )
    {
        this.unit = unit;
    }

    @Override
    public Long call()
    {
        unit.run();

        return System.currentTimeMillis();
    }

}
