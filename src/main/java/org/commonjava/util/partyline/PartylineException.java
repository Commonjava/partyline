package org.commonjava.util.partyline;

import java.io.IOException;

public class PartylineException
                extends IOException
{
    public PartylineException( String message, Throwable cause )
    {
        super( message, cause );
    }
}
