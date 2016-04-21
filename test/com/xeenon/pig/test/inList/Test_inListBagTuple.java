package com.xeenon.pig.test.inList;

import com.xeenon.pig.inList;
import com.xeenon.pig.inListBagTuple;
import junit.framework.Assert;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

/**
 * Created with IntelliJ IDEA.
 * User: xeenon
 * Date: 23.08.13
 * Time: 22:27
 */
public class Test_inListBagTuple {
    String[] test = {"one", "two", "three", "four", "five"};
    String must = "two,five";
    String mustNot = "six,seven,abra-kadabra";

    @Test
    public void testString() throws Exception {
        inListBagTuple filterMust = new inListBagTuple(must, ",");
        inListBagTuple filterMustNot = new inListBagTuple(mustNot, ",");

        int countMust = 0;
        int countMustNot = 0;

        DataBag bag = BagFactory.getInstance().newDefaultBag();
        for (String str : test) {

            Tuple tuple = TupleFactory.getInstance().newTuple(1);
            tuple.set(0, str);

            bag.add(tuple);
        }

        Tuple input = TupleFactory.getInstance().newTuple(2);
        input.set(0, bag);
        input.set(1, 0);
        if (filterMust.exec(input))
            countMust++;

        if (filterMustNot.exec(input))
            countMustNot++;

        Assert.assertEquals("positive filter", countMust, must.split(",").length);
        Assert.assertEquals("negative filter", countMustNot, 0);
    }

    @Test
    public void testFile() throws Exception {
    }
}
