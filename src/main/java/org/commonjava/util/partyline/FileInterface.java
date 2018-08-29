package org.commonjava.util.partyline;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface FileInterface
{

    int read()
        throws IOException;

    int write( ByteBuffer buf )
        throws IOException;

    void close()
        throws IOException;

}