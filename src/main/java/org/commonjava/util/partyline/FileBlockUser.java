package org.commonjava.util.partyline;


public class FileBlockUser
{

    public FileBlock fileBlock;

    public int userCount;

    public FileBlockUser( FileBlock fileBlock )
    {
        this.fileBlock = fileBlock;
        this.userCount = 1;
    }

}