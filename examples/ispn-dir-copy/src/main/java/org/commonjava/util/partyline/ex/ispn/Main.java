package org.commonjava.util.partyline.ex.ispn;

import org.apache.commons.io.IOUtils;
import org.commonjava.util.partyline.Partyline;
import org.commonjava.util.partyline.impl.infinispan.model.FileBlock;
import org.commonjava.util.partyline.impl.infinispan.model.FileMeta;
import org.commonjava.util.partyline.impl.infinispan.model.InfinispanJFS;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.transaction.TransactionMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class Main
{

    public static void main(String[] args)
    {
        if ( args.length < 2 )
        {
            System.out.println( "Usage: $0 <input-dir> <output-dir>");
            System.exit( 1 );
        }

        File indir = new File( args[0] );
        File outdir = new File( args[1] );

        int blockSize = 1024;

        Logger logger = LoggerFactory.getLogger( Main.class );
        logger.info( "Copying files from: " + indir + " to: " + outdir );

        DefaultCacheManager cacheManager = new DefaultCacheManager( true );
        cacheManager.defineConfiguration( "blocks", new ConfigurationBuilder().transaction().transactionMode( TransactionMode.TRANSACTIONAL ).build() );
        Cache<String, FileBlock> blocks = cacheManager.getCache( "blocks", true );

        cacheManager.defineConfiguration( "files", new ConfigurationBuilder().transaction().transactionMode( TransactionMode.TRANSACTIONAL ).build() );
        Cache<String, FileMeta> files = cacheManager.getCache( "files", true );

        Partyline partyline = new Partyline( new InfinispanJFS( "single-node", files, blocks, blockSize ) );

        AtomicInteger inCounter = new AtomicInteger();
        File[] dirFiles = indir.listFiles();

        // copy from input dir to ISPN
        Stream.of( dirFiles ).parallel().forEach( dirFile->{
            if ( !dirFile.isDirectory() )
            {
                logger.info( "Copying from input: {}", dirFile );

                try(InputStream in = new FileInputStream( dirFile ); OutputStream out = partyline.openOutputStream( dirFile ) )
                {
                    // copy writes and stops when -1 is read
                    IOUtils.copy( in, out );
                    // workaround - write -1 to indicate EOF
                    out.write(-1);
                }
                catch ( InterruptedException e )
                {
                    logger.error( "Input copy interrupted: " + dirFile.getName(), e );
                }
                catch ( IOException e )
                {
                    logger.error( "Failed to copy to input: " + dirFile.getName(), e );
                }
                finally
                {
                    inCounter.incrementAndGet();
                }
            }
        } );

        AtomicInteger outCounter = new AtomicInteger();

        // pull the same files from ISPN and copy to output dir
        Stream.of( dirFiles ).parallel().forEach( dirFile->{
            if ( !dirFile.isDirectory() )
            {
                logger.info( "Copying to output: {}", dirFile );

                try (InputStream in = partyline.openInputStream( dirFile );
                     OutputStream out = new FileOutputStream( new File( outdir, dirFile.getName() ) ))
                {
                    IOUtils.copy( in, out );
                }
                catch ( InterruptedException e )
                {
                    logger.error( "Output copy interrupted: " + dirFile.getName(), e );
                }
                catch ( IOException e )
                {
                    logger.error( "Failed to copy to output: " + dirFile.getName(), e );
                }
                finally
                {
                    outCounter.incrementAndGet();
                }
            }
        } );

        System.out.println( "Copied " + inCounter.get() + " files into Infinispan, and " + outCounter + " back out to destination directory." );
    }
}
