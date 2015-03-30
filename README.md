# PartyLine Joinable Streams

PartyLine is a tiny library for allowing code to attach ("join") a stream that's already in progress. This is particularly useful on servers that cache content. Caching large files as a result of a user request can easily lead to request timeouts for the user, if the server downloads the content into the cache and then serves it to the user. Even if your server sets up a simple "tee" output stream to simultaneously push the content back to the original requestor and to the cache, there is still a problem with any other users that come along looking for that content while the caching download is in progress. In this case, the problem has been pushed off to the next user, whose request may timeout while the first user and the cache receive the content, simply because they cannot join the stream already in progress.

## The Stream Itself

PartyLine solves this by maintaining a RandomAccessFile and associated FileChannel under the covers. It wraps these in an OutputStream implementation. As new content is written to the OutputStream wrapper, it is pushed to an internal buffer. When the buffer is full, it's pushed to the FileChannel. The total number of bytes written (both to disk and to the buffer) and the number of bytes purged (to disk) are tracked. Meanwhile, other threads in the application can "join" the stream and receive an InputStream implementation. This InputStream pulls chunks of data from the channel into a buffer, being careful never to exceed the purged-byte count in the associated OutputStream. Each time the OutputStream flushes the buffer to disk, it notifies associated InputStreams that there is new content they can buffer and serve to callers.

### Example Usage

    InputStream trunkIn = // wherever you get your content...
    JoinableOutputStream out = new JoinableOutputStream(new File("/path/to/my.file.txt"));
    
    CountDownLatch cdl = new CountDownLatch(2);
    Runnable r = new Runnable(){
        public void run(){
            try{
                IOUtils.copy( trunkIn, out );
            }catch(IOException e){
                e.printStackTrace();
            }
            cdl.countDown();
        }
    };
    Thread t = new Thread(r);
    t.start();
    
    Runnable r2 = new Runnable(){
        public void run(){
            try{
                System.out.println(Thread.currentThread().getName()+": "+IOUtils.toString(in));
            }catch(IOException e){
                e.printStackTrace();
            }
            cdl.countDown();
        }
    };
    Thread t2 = new Thread(r2);
    t2.start();
    
    // wait for everything to end.
    cdl.await();

## The File Manager

In addition to joinable streams, it's often useful to have a single manager component that will manage the locks on a given file, and prevent people
from performing lower-level operations while something like the joinable output stream is in use.

### Example Usage

    JoinableFileManager mgr = new JoinableFileManager();
    
    File f = new File("/path/to/my.file.txt");
    
    // Really a JoinableOutputStream
    OutputStream out = mgr.openOutputStream(f);
    
    // Really a JoinInputStream related to the above output stream
    InputStream in = mgr.openInputStream(f);
    
    // do stuff, or...
    
    //false, you can get an InputStream joined to the current output stream.
    boolean readLocked = mgr.isReadLocked(f);
    
    //true, we already have an active output stream
    boolean writeLocked = mgr.isWriteLocked(f);
    
    OutputStream out2 = mgr.openOutputStream(f); // waits until 'out' above closes.
    
    // waits for in to close...in this example, infinitely (unless another thread closes 'in')
    out.close();
    
    // Success! It was unlocked by the close() operation above.
    OutputStream out2 = mgr.openOutputStream(f);
    
    mgr.lock(f); // returns false, because there is already an active stream to that file
    
    File f2 = new File("/path/to/another.file.txt");
    mgr.lock(f2); // return true.
    
    OutputStream out3 = mgr.openOutputStream(f2); // waits until manually unlocked.
    
    mgr.unlock(f2); // Okay, NOW we can open an output stream if we want.

