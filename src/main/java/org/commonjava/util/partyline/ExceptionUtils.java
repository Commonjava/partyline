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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

class ExceptionUtils
{
    static void handleError( AtomicReference<Exception> error, String label )
            throws IOException, InterruptedException
    {
        Exception e = error.get();
        if ( e != null )
        {
            if ( e instanceof IOException )
            {
                throw (IOException) e;
            }

            if ( e instanceof InterruptedException )
            {
                throw (InterruptedException) e;
            }

            if ( e instanceof RuntimeException )
            {
                throw (RuntimeException) e;
            }

            throw new IOException( "Operation failed: " + label, e );
        }
    }
}
