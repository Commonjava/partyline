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

import java.io.IOException;
import java.io.InputStream;

public class CallbackInputStream
    extends InputStream
{

    private final InputStream delegate;

    private final StreamCallbacks callbacks;

    public CallbackInputStream( final InputStream delegate, final StreamCallbacks callbacks )
    {
        this.delegate = delegate;
        this.callbacks = callbacks;
    }

    @Override
    public int available()
        throws IOException
    {
        return delegate.available();
    }

    @Override
    public void close()
        throws IOException
    {
        delegate.close();
        callbacks.closed();
    }

    @Override
    public void mark( final int readlimit )
    {
        delegate.mark( readlimit );
    }

    @Override
    public boolean markSupported()
    {
        return delegate.markSupported();
    }

    @Override
    public int read( final byte[] b, final int off, final int len )
        throws IOException
    {
        return delegate.read( b, off, len );
    }

    @Override
    public int read( final byte[] b )
        throws IOException
    {
        return delegate.read( b );
    }

    @Override
    public void reset()
        throws IOException
    {
        delegate.reset();
    }

    @Override
    public long skip( final long n )
        throws IOException
    {
        return delegate.skip( n );
    }

    @Override
    public String toString()
    {
        return "Callback-wrapped: " + delegate.toString();
    }

    @Override
    public int read()
        throws IOException
    {
        return delegate.read();
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
