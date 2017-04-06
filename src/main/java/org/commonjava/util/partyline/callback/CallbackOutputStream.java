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
package org.commonjava.util.partyline.callback;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

@Deprecated
public class CallbackOutputStream
    extends FilterOutputStream
{

    private final StreamCallbacks callbacks;
    private final OutputStream delegate;

    public CallbackOutputStream( final OutputStream delegate, final StreamCallbacks callbacks )
    {
        super( delegate );
        this.delegate = delegate;
        this.callbacks = callbacks;
    }

    @Override
    public void close()
        throws IOException
    {
        if ( callbacks != null )
        {
            callbacks.beforeClose();
        }

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
    public void flush()
        throws IOException
    {
        try
        {
            super.flush();
        }
        finally
        {
            if ( callbacks != null )
            {
                callbacks.flushed();
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
        final CallbackOutputStream other = (CallbackOutputStream) obj;
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
