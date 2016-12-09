package org.commonjava.util.partyline;

import java.io.IOException;

/**
 * Created by jdcasey on 12/8/16.
 */
@FunctionalInterface
interface LockedFileOperation<T>
{
    T execute( FileOperationLock opLock )
            throws InterruptedException, IOException;
}
