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

import org.commonjava.util.partyline.lock.LockLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Date;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class FileMeta
                implements Externalizable
{
    private String filePath;

    private boolean directory;

    private Date createdDate;

    private Date lastModifiedDate;

    private FileBlock firstBlock;

    private int blockSize;

    private Map<String, String> metadataMap = new ConcurrentHashMap<>();

    private Map<String, LockLevel> lockMap = new ConcurrentHashMap<>();

    public FileMeta(){}

    public FileMeta( String path, boolean directory, int blockSize )
    {
        this.directory = directory;
        this.filePath = path;

        this.createdDate = new Date();
        this.lastModifiedDate = (Date) this.createdDate.clone();

        this.blockSize = blockSize;

        Logger logger = LoggerFactory.getLogger( getClass() );
        logger.trace( "Created FileMeta {} at date {}", filePath, createdDate.toString() );
    }

    public boolean isDirectory()
    {
        return directory;
    }

    public FileBlock createBlock( UUID blockID, boolean first )
    {
        FileBlock block = new FileBlock( filePath, blockID.toString(), blockSize );

        if ( first )
        {
            this.firstBlock = block;
            lastModifiedDate = new Date();
        }

        return block;
    }

    public void setLock( String nodeID, LockLevel level )
    {
        lockMap.put( nodeID, level );
    }

    public void removeLock( String nodeID )
    {
        lockMap.remove( nodeID );
    }

    public LockLevel getLockLevel( String nodeID )
    {
        return lockMap.get( nodeID );
    }

    Date getCreatedDate()
    {
        return createdDate;
    }

    Date getLastModifiedDate()
    {
        return lastModifiedDate;
    }

    public int getBlockSize()
    {
        return blockSize;
    }

    public FileBlock getFirstBlock()
    {
        return firstBlock;
    }

    public String getFilePath()
    {
        return filePath;
    }

    public void writeExternal( ObjectOutput out ) throws IOException
    {
        out.writeUTF( filePath );
        out.writeBoolean( directory );
        out.writeObject( createdDate );
        out.writeObject( lastModifiedDate );
        out.writeInt( blockSize );
        out.writeObject( metadataMap );
        out.writeObject( lockMap );
        out.writeObject( firstBlock );
    }

    public void readExternal( ObjectInput in ) throws IOException, ClassNotFoundException
    {
        filePath = in.readUTF();
        directory = in.readBoolean();
        createdDate = (Date) in.readObject();
        lastModifiedDate = (Date) in.readObject();
        blockSize = in.readInt();
        metadataMap = (ConcurrentHashMap<String, String>) in.readObject();
        lockMap = (ConcurrentHashMap<String, LockLevel>) in.readObject();
        firstBlock = (FileBlock) in.readObject();
    }
}