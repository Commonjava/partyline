package org.commonjava.util.partyline;

import org.junit.ClassRule;
import org.junit.rules.TestRule;

import java.util.concurrent.TimeUnit;

import static org.commonjava.util.partyline.fixture.ThreadDumper.timeoutRule;

/**
 * Created by jdcasey on 12/8/16.
 */
public class AbstractBytemanTest
        extends AbstractJointedIOTest
{

    @ClassRule
    public static TestRule timeout = timeoutRule( 30, TimeUnit.SECONDS );

}
