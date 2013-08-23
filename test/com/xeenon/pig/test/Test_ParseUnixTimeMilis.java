package com.xeenon.pig.test;

import com.xeenon.pig.ParseUnixTimeMilis;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: xeenon
 * Date: 22.08.13
 * Time: 9:24
 */
public class Test_ParseUnixTimeMilis {
    public Map<String, Long> ips = new HashMap<>();
    public TupleFactory tupleFactory = TupleFactory.getInstance();

    public Test_ParseUnixTimeMilis() {
        ips.put("2013-08-22 11", 1377157271l);
        ips.put("2002-03-05 23", 1015360934l);
        ips.put("2013-07-25 17", 1374759794l);
    }

    @Test
    public void date() throws Exception {
        ParseUnixTimeMilis parse = new ParseUnixTimeMilis();
        for (Map.Entry<String, Long> entry : ips.entrySet()) {
            Tuple input = tupleFactory.newTuple();
            input.append(entry.getValue() * 1000l);

            Tuple result = parse.exec(input);
            Tuple expect = tupleFactory.newTuple();
            expect.append(entry.getKey().split(" ")[0]);
            expect.append(Integer.valueOf(entry.getKey().split(" ")[1]));

            Assert.assertEquals(expect, result);
        }
    }

}
