---
title: Locking Specification
---

###Introduction

Partyline has some specific requirements for file locking, mainly owing to the environment it was designed to serve: server-side applications that deal heavily in file content. In this environment, it's fairly common for the activities spawned by a user request to span multiple threads, and even continue beyond the response that the user receives. Additionally, in order to maximize performance, these applications must maintain the lightest touch possible on the filesystem when managing content, locking only when necessary and to the minimum extent required. This requires an advanced approach to file locking.

It's fair to say that file locking is at the heart of what Partyline does.

###Locks: Exclusive, Non-Exclusive...and Semi-Exclusive?

Many readers will be familiar with the idea of exclusive and non-exclusive file locks. Exclusive locks won't allow any other access for the duration of the lock, while non-exclusive locks allow multiple concurrent accesses (usually just for reads or other operations that preserve immutability of the data). But what do you do when you want to read data while it's being written, *and* start reading a file while it's still in the process of being written?

Joinable writes are really scenarios where we have one writer and zero or more readers. We cannot allow multiple simultaneous writers for obvious reasons, but when a file is locked for writing it *is* still available for reading. So, if the first uesr is a writer, Partyline can also allow non-exclusive reader locks. If another writer tries to access the file, the lock will act like an exclusive lock.

On the other hand, if the first user is a reader, only other readers will be allowed access to the file until all readers are done. So in this case, the lock is non-exclusive for readers, but will exclude writers.

A third scenario is when a user attempts to lock a file for deletion. If there are any other locks on the file, the delete lock will wait until timeout, and if the other locks haven't cleared by then, it will fail. If the first user obtains a delete lock on a file, no other locks are granted until the delete lock is released. Deletion is an absolute exclusive lock, in contrast to write locks. It will remove the file, so not even readers are allowed access during the operation.

###Directory Locking

Further complicating matters in the file-locking world, it's possible that a user may need to address (read, write, delete) all the files in directory at once. When the first user locks a directory, any lock attempt by subsequent users should consider the entire sub-directory tree (along with the files it contains) to be locked at the same level. In other words, if the first user delete-locks a directory, nobody else can obtain any lock on that directory or any of its descendants until the lock is cleared. If the directory is write-locked, then readers can access content, but not other writers or deletors.

On the other hand, if the first user locks a file, any subsequent attempts to lock one of its ancestor directories will act as if the directory already had a read lock in place on it. This will allow users to read the contents of the directory, but not obtain a write or delete lock on the ancestor directories themselves. They can still obtain write or delete locks on other *files* within the directory, just not on the directories.

When all locks on the files are released for a given subtree, these directories can be locked as described above.
