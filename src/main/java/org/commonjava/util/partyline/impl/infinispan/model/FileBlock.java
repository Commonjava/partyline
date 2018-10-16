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
import java.util.UUID;

public class FileBlock
        implements Externalizable
{
    private String fileID;

    private UUID blockID;

    private UUID nextBlockID;

    private Date createdDate;

    private Date lastModifiedDate;

    private boolean eof = false;

    private static final int BLOCK_SIZE = 1024 * 1024 * 8; // 8mb

    private ByteBuffer data = ByteBuffer.allocateDirect( BLOCK_SIZE );

    FileBlock( String fileID, UUID blockID )
    {
        this( fileID, blockID, null );
    }

    FileBlock( String fileID, UUID blockID, UUID nextBlockID )
    {
        this.fileID = fileID;
        this.blockID = blockID;
        this.nextBlockID = nextBlockID;

        this.createdDate = new Date();
        this.lastModifiedDate = (Date) this.createdDate.clone();

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

    public UUID getBlockID()
    {
        return blockID;
    }

    public UUID getNextBlockID()
    {
        return nextBlockID;
    }

    public void setNextBlockID( UUID blockID )
    {
        nextBlockID = blockID;
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

    public void writeToBuffer( Byte b )
    {
        data.put( b );
    }

    public void writeExternal( ObjectOutput out )
            throws IOException
    {
        out.writeUTF( fileID );
        out.writeObject( blockID );
        out.writeObject( nextBlockID );
        out.writeObject( createdDate );
        out.writeObject( lastModifiedDate );
        out.writeInt( data.capacity() );
        out.write( data.array() );
    }

    public void readExternal( ObjectInput in )
            throws IOException, ClassNotFoundException
    {
        fileID = in.readUTF();
        blockID = (UUID) in.readObject();
        nextBlockID = (UUID) in.readObject();
        createdDate = (Date) in.readObject();
        lastModifiedDate = (Date) in.readObject();
        int bufferCapacity = in.readInt();
        byte[] buffer = new byte[bufferCapacity];
        in.read( buffer, 0, bufferCapacity );
        data = ByteBuffer.wrap( buffer, 0, bufferCapacity );
    }
}