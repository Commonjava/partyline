package org.commonjava.util.partyline.fixture;

import org.apache.commons.lang.StringUtils;
import org.junit.rules.TestName;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.junit.runners.model.Statement;

import java.lang.management.ManagementFactory;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.stream.Stream;

import static org.apache.commons.lang.StringUtils.join;

/**
 * Created by jdcasey on 11/28/16.
 */
public final class ThreadDumper
{
    private ThreadDumper()
    {
    }

    public static void dumpThreads()
    {
        StringBuilder sb = new StringBuilder();
        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        ThreadInfo[] threadInfos = threadMXBean.getThreadInfo( threadMXBean.getAllThreadIds(), 100 );
        Stream.of( threadInfos ).forEachOrdered( ( ti ) -> {
            if ( sb.length() > 0 )
            {
                sb.append( "\n\n" );
            }

            sb.append( ti.getThreadName() )
              .append( "\n  State: " )
              .append( ti.getThreadState() )
              .append( "\n  Lock Info: " )
              .append( ti.getLockInfo() )
              .append( "\n  Monitors:" );

            MonitorInfo[] monitors = ti.getLockedMonitors();
            if ( monitors == null || monitors.length < 1 )
            {
                sb.append("  -NONE-");
            }
            else
            {
                sb.append( "\n  - " ).append( join( monitors, "\n  - " ) );
            }

            sb.append( "\n  Trace:\n    " )
              .append( join( ti.getStackTrace(), "\n    " ) );

        } );

        System.out.println( sb );
    }

    public static TestRule wrap( TestName name, Timeout build )
    {
        return ( base, description ) -> new ThreadDumpWrapper( name, build.apply( base, description ) );
    }

    private static class ThreadDumpWrapper
            extends Statement
    {
        private final String testMethod;

        private Statement base;

        public ThreadDumpWrapper( TestName name, Statement base )
        {
            this.testMethod = name.getMethodName();
            this.base = base;
        }

        @Override
        public void evaluate()
                throws Throwable
        {
            System.out.println( "Test timeout: " + testMethod + ". Thread dump: ");
            dumpThreads();

            base.evaluate();
        }
    }
}
