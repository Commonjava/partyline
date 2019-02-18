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
package org.commonjava.util.partyline.impl.infinispan.model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import java.util.Date;

public class FileBlock
        implements Externalizable
{
    private String fileID;

    private String blockID;

    private String nextBlockID;

    private Date createdDate;

    private Date lastModifiedDate;

    private boolean eof = false;

    private ByteBuffer data;

    private int blockSize;

    public FileBlock(){}

    public FileBlock( String fileID, String blockID, int blockSize )
    {
        this( fileID, blockID, blockSize, null );
    }

    FileBlock( String fileID, String blockID, int blockSize, String nextBlockID )
    {
        this.fileID = fileID;
        this.blockID = blockID;
        this.nextBlockID = nextBlockID;

        this.createdDate = new Date();
        this.lastModifiedDate = (Date) this.createdDate.clone();

        this.blockSize = blockSize;

        data = ByteBuffer.allocateDirect( this.blockSize );

        Logger logger = LoggerFactory.getLogger( getClass() );
        logger.trace( "Created FileMeta {} at date {}", fileID.toString(), createdDate.toString() );
    }

    public void setEOF()
    {
        eof = true;
        nextBlockID = null;
        lastModifiedDate = new Date();
    }

    public Date getCreatedDate()
    {
        return createdDate;
    }

    public Date getLastModifiedDate()
    {
        return lastModifiedDate;
    }

    public String getBlockID()
    {
        return blockID;
    }

    public String getNextBlockID()
    {
        return nextBlockID;
    }

    public void setNextBlockID( String blockID )
    {
        nextBlockID = blockID;
    }

    public int readFromBuffer()
    {
        return data.get() & 0xff;
    }

    public ByteBuffer getBuffer()
    {
        return data;
    }

    byte[] getByteArray()
    {
        if ( data.hasArray() )
        {
            return data.array();
        }
        else
        {
            byte[] arr = new byte[data.capacity()];
            // We need to try to preserve the position/limit of data
            int originalPosition = data.position();
            int originalLimit = data.limit();
            data.get( arr, 0, data.limit() );
            data.position( originalPosition );
            data.limit( originalLimit );
            return arr;
        }
    }

    public String getFileID()
    {
        return fileID;
    }

    public boolean isEOF()
    {
        return eof;
    }

    public boolean full()
    {
        return ( data.position() == data.capacity() );
    }

    public boolean hasRemaining()
    {
        return ( data.position() == data.limit() );
    }

    public void writeToBuffer( Byte b )
    {
        data.put( b );
    }

    public void writeExternal( ObjectOutput out )
            throws IOException
    {
        out.writeInt( blockSize );
        out.writeUTF( fileID );
        out.writeUTF( blockID );
        if ( nextBlockID == null )
        {
            nextBlockID = "null";
        }
        out.writeUTF( nextBlockID );
        out.writeObject( createdDate );
        out.writeObject( lastModifiedDate );
        out.writeBoolean( eof );
        out.writeInt( data.capacity() );
        out.writeInt( data.limit() );
        out.write( getByteArray() );
    }

    public void readExternal( ObjectInput in )
            throws IOException, ClassNotFoundException
    {
        blockSize = in.readInt();
        fileID = in.readUTF();
        blockID = in.readUTF();
        nextBlockID = in.readUTF();
        if ( nextBlockID == "null" )
        {
            nextBlockID = null;
        }
        createdDate = (Date) in.readObject();
        lastModifiedDate = (Date) in.readObject();
        eof = in.readBoolean();
        int bufferCapacity = in.readInt();
        int bufferLimit = in.readInt();
        byte[] buffer = new byte[bufferCapacity];
        in.read( buffer, 0, bufferLimit );
        data.allocate( bufferCapacity );
        data = ByteBuffer.wrap( buffer );
    }
}