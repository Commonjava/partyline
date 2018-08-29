package org.commonjava.util.partyline;

import java.util.UUID;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class InfinispanFS
{

    private Map<UUID, FileBlock> blockMap = new ConcurrentHashMap<>();


}


