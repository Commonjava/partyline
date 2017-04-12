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

/**
 * Enumerates the types of locks that are allowed on files in partyline. File locks are <b>SOMETIMES</b> non-exclusive.
 * This locking approach is aimed at allowing concurrent reads, even when a file is currently being written (the entire
 * purpose of partyline).
 * <br/>
 * Lock exclusivity follows these rules:
 * <ul>
 *     <li>delete locks are exclusive</li>
 *     <li>write locks allow concurrent read locks, but <b>not</b> delete locks or other write locks</li>
 *     <li>a read lock established <b>as the first lock on the stream</b> prohibits both delete and write locks</li>
 *     <li>read locks allow concurrent read locks</li>
 * </ul>
 */
public enum LockLevel
{
    read,
    write,
    delete;
}
