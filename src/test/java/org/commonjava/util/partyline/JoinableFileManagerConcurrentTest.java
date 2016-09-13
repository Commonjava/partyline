package org.commonjava.util.partyline;

import org.apache.commons.io.IOUtils;
import org.jboss.byteman.contrib.bmunit.BMScript;
import org.jboss.byteman.contrib.bmunit.BMUnitConfig;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

@RunWith( org.jboss.byteman.contrib.bmunit.BMUnitRunner.class )
@BMUnitConfig( loadDirectory = "target/test-classes/bmunit", debug = true )
public class JoinableFileManagerConcurrentTest
{
    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    private final ExecutorService testPool = Executors.newFixedThreadPool( 2 );

    private final CountDownLatch latch = new CountDownLatch( 2 );

    private final JoinableFileManager fileManager = new JoinableFileManager();

    private void latchWait( CountDownLatch latch )
    {
        try
        {
            latch.await();
        }
        catch ( InterruptedException e )
        {
            System.out.println( "Threads await Exception." );
        }
    }

    private void writeFile( File file, String content )
            throws IOException
    {
        final OutputStream out = new FileOutputStream( file );
        IOUtils.write( content, out );
        out.close();
    }

    @Test
    @BMScript( "TryToBothRead.btm" )
    public void testTryToBothRead()
            throws Exception
    {
        final String content = "This is a bmunit test";
        final File file = temp.newFile( "file_both_read.txt" );
        writeFile( file, content );
        final Future<String> readingFuture1 =
                testPool.submit( (Callable<String>) new ReadTask( fileManager, content, file, latch ) );
        final Future<String> readingFuture2 =
                testPool.submit( (Callable<String>) new ReadTask( fileManager, content, file, latch ) );

        latchWait( latch );

        final String readingResult1 = readingFuture1.get();
        assertThat( readingResult1, equalTo( content ) );
        final String readingResult2 = readingFuture2.get();
        assertThat( readingResult2, equalTo( content ) );
    }

    private abstract class IOTask
            implements Runnable
    {
        protected JoinableFileManager fileManager;

        protected String content;

        protected CountDownLatch controlLatch;

        protected long waiting;

        protected File file;

        protected IOTask( JoinableFileManager fileManager, String content, File file, CountDownLatch controlLatch,
                          long waiting )
        {
            this.fileManager = fileManager;
            this.content = content;
            this.file = file;
            this.controlLatch = controlLatch;
            this.waiting = waiting;
        }
    }

    private final class ReadTask
            extends IOTask
            implements Callable<String>
    {
        private String readingResult;

        public ReadTask( JoinableFileManager fileManager, String content, File file, CountDownLatch controlLatch )
        {
            super( fileManager, content, file, controlLatch, -1 );
        }

        public ReadTask( JoinableFileManager fileManager, String content, File file, CountDownLatch controlLatch,
                         long waiting )
        {
            super( fileManager, content, file, controlLatch, waiting );
        }

        @Override
        public void run()
        {
            try
            {
                final InputStream in = fileManager.openInputStream( file );
                if ( in == null )
                {
                    System.out.println( "Can not read content as the input stream is null." );
                    controlLatch.countDown();
                    return;
                }
                final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                int read = -1;
                final byte[] buf = new byte[512];
                System.out.println(
                        String.format( "<<<ReadTask>>> will start to read from the resource with inputStream %s",
                                       in.getClass().getName() ) );
                while ( ( read = in.read( buf ) ) > -1 )
                {
                    if ( waiting > 0 )
                    {
                        Thread.sleep( waiting );
                    }
                    baos.write( buf, 0, read );
                }
                baos.close();
                in.close();
                System.out.println( String.format( "<<<ReadTask>>> reading from the resource done with inputStream %s",
                                                   in.getClass().getName() ) );
                readingResult = new String( baos.toByteArray(), "UTF-8" );
                controlLatch.countDown();
            }
            catch ( Exception e )
            {
                System.out.println( "Read Task Runtime Exception." );
                e.printStackTrace();
            }
        }

        @Override
        public String call()
        {
            this.run();
            return readingResult;
        }
    }
}
