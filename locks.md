---
title: Locking Specification
---

###Introduction

Partyline has some specific requirements for file locking, mainly owing to its "native" environment: server-side applications that deal heavily in file content. In this environment, it's fairly common for the activities spawned by a user request to span multiple threads, and even continue beyond the response that the user receives. Additionally, in order to maximize performance, these applications must maintain the lightest touch possible on the filesystem when managing content, locking only when necessary and to the minimum extent required. This requires an advanced approach to file locking.

It's fair to say that file locking is at the heart of what Partyline does.

###Locked, but Who Has the Key?

Most locking mechanisms tie the lock back to a given thread, either by storing the lock in a ThreadLocal, or by some other means. However, this isn't adequate for applications like Indy, whose processing of a given file may start with a user request and extend beyond the corresponding response, into the sequence of events that are triggered asynchronously as a result of the request. 

It helps to have an example:

Imaging a user makes request to read a file from Indy. Just to keep things simple, let's assume the file already exists, and doesn't need to be downloaded from a remote upstream site. The thread servicing the user's request retrieves a reference to the file in question and sends back a streaming response. When the file is opened for reading, Indy also fires a FileAccessedEvent, which triggers several listeners observing that type of event, and kicks off a few post-processing activities.

