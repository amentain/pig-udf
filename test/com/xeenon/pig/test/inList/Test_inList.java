package com.xeenon.pig.test.inList;

import com.xeenon.pig.inList;
import junit.framework.Assert;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

/**
 * Created with IntelliJ IDEA.
 * User: xeenon
 * Date: 23.08.13
 * Time: 22:27
 */
public class Test_inList {
    String[] test = {"one", "two", "three", "four", "five"};
    String must = "two,five";
    String mustNot = "six,seven";

    @Test
    public void testString() throws Exception {
        inList filterMust = new inList(must, ",");
        inList filterMustNot = new inList(mustNot, ",");

        int mustCount = 0;
        int mustNotCount = 0;
        Tuple input = TupleFactory.getInstance().newTuple(1);
        for (String str : test) {
            input.set(0, str);
            if (filterMust.exec(input))
                mustCount++;

            if (filterMustNot.exec(input))
                mustNotCount++;
        }

        Assert.assertEquals("positive filter", mustCount, must.split(",").length);
        Assert.assertEquals("negative filter", mustNotCount, 0);
    }
}
