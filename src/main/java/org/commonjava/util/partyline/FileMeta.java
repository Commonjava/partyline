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
package org.commonjava.util.partyline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.Externalizable;
import java.util.Map;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class FileMeta
        implements Externalizable
{
    private UUID fileID;

    private String path;

    private Date createdDate;

    private Date lastModifiedDate;

    private FileBlock firstBlock;

    private Map<String, String> metadataMap = new ConcurrentHashMap<>();

    private Map<String, LockLevel> lockMap = new ConcurrentHashMap<>();

    FileMeta( String path, UUID fileID)
    {
        this.path = path;
        this.fileID = fileID;

        this.createdDate = new Date();
        this.lastModifiedDate = (Date) this.createdDate.clone();

        Logger logger = LoggerFactory.getLogger( getClass() );
        logger.trace( "Created FileMeta {} at date {}", fileID.toString(), createdDate.toString() );
    }

    public FileBlock createBlock( UUID blockID, boolean first )
    {
        FileBlock block = new FileBlock(this.fileID, UUID.randomUUID());

        if ( first )
        {
            this.firstBlock = block;
            lastModifiedDate = new Date();
        }

        return block;
    }

    void addLock( String nodeID, LockLevel level )
    {
        lockMap.put( nodeID, level );
    }

    String getPath()
    {
        return path;
    }

    Date getCreatedDate()
    {
        return createdDate;
    }

    Date getLastModifiedDate()
    {
        return lastModifiedDate;
    }

    FileBlock getFirstBlock()
    {
        return firstBlock;
    }

    public UUID getFileID()
    {
        return fileID;
    }

    public void writeExternal(ObjectOutput out)
                throws IOException
    {
        out.writeObject( fileID );
        out.writeUTF( path );
        out.writeObject( createdDate );
        out.writeObject( lastModifiedDate );
        out.writeObject( firstBlock );
        out.writeObject( metadataMap );
        out.writeObject( lockMap );
    }

    public void readExternal(ObjectInput in)
                throws IOException, ClassNotFoundException
    {
        fileID = (UUID) in.readObject();
        path = in.readUTF();
        createdDate = (Date) in.readObject();
        lastModifiedDate = (Date) in.readObject();
        firstBlock = (FileBlock) in.readObject();
        metadataMap = (Map<String, String>) in.readObject();
        lockMap = (Map<String, LockLevel>) in.readObject();
    }
}