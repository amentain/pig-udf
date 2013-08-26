package com.xeenon.pig.test.ipFilter;

import com.xeenon.pig.IPFilter;
import junit.framework.Assert;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: xeenon
 * Date: 24.08.13
 * Time: 15:55
 */
public class Test_IPFilter {
    String[] test = {
            "5.165.64.1",
            "188.187.233.0",
            "188.187.233.10",
            "188.187.233.255"
    };

    String positive_1 = "188.187.233.10,188.187.233.255";
    String positive_2 = "188.187.233.0-188.187.233.2,5.165.64.0-5.165.64.10";

    String negative_1 = "127.0.0.1,255.0.0.0";
    String negative_2 = "199.0.0.1-200.0.0.1,5.165.64.2-5.165.64.3";

    @Test
    public void simpleTest() throws Exception {
        Assert.assertEquals("positive_1", positive_1.split(",").length, test(new IPFilter(positive_1, ",")));
        Assert.assertEquals("positive_2", positive_2.split(",").length, test(new IPFilter(positive_2, ",")));

        Assert.assertEquals("negative_1", 0, test(new IPFilter(negative_1, ",")));
        Assert.assertEquals("negative_2", 0, test(new IPFilter(negative_2, ",")));
    }

    private int test(IPFilter filter) throws IOException {
        int matches = 0;
        Tuple tuple = TupleFactory.getInstance().newTuple(1);
        for (String str : test) {
            tuple.set(0, str);
            if (filter.exec(tuple) != null) {
                matches++;
            }
        }
        return matches;
    }
}
