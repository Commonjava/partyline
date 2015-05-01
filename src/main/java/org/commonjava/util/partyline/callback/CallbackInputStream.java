/*******************************************************************************
* Copyright (c) 2015 ${owner}
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the GNU Public License v3.0
* which accompanies this distribution, and is available at
* http://www.gnu.org/licenses/gpl.html
*
* Contributors:
* ${owner} - initial API and implementation
******************************************************************************/
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
package org.commonjava.util.partyline.callback;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

public class CallbackInputStream
    extends FilterInputStream
{

    private final InputStream delegate;

    private final StreamCallbacks callbacks;

    public CallbackInputStream( final InputStream delegate, final StreamCallbacks callbacks )
    {
        super( delegate );
        this.delegate = delegate;
        this.callbacks = callbacks;
    }

    @Override
    public void close()
        throws IOException
    {
        try
        {
            super.close();
        }
        finally
        {
            if ( callbacks != null )
            {
                callbacks.closed();
            }
        }
    }

    @Override
    public String toString()
    {
        return "Callback-wrapped: " + delegate.toString();
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( delegate == null ) ? 0 : delegate.hashCode() );
        return result;
    }

    @Override
    public boolean equals( final Object obj )
    {
        if ( this == obj )
        {
            return true;
        }
        if ( obj == null )
        {
            return false;
        }
        if ( getClass() != obj.getClass() )
        {
            return false;
        }
        final CallbackInputStream other = (CallbackInputStream) obj;
        if ( delegate == null )
        {
            if ( other.delegate != null )
            {
                return false;
            }
        }
        else if ( !delegate.equals( other.delegate ) )
        {
            return false;
        }
        return true;
    }

}
