package com.xeenon.pig.test.simple;

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
        ips.put("2013-08-22 11 41", 1377157271l);
        ips.put("2002-03-05 23 42", 1015360934l);
        ips.put("2013-07-25 17 43", 1374759794l);
        ips.put("2013-09-07 23 59", 1378583999l);
        ips.put("2013-09-08 00 00", 1378584000l);
        ips.put("2013-09-08 01 00", 1378587600l);
        ips.put("2013-09-08 00 59", 1378587599l);
        ips.put("2013-09-11 00 00", 1378843200l);
        ips.put("2013-09-16 00 00", 1379275200l);
        ips.put("2013-09-19 00 00", 1379534400l);
        ips.put("2013-09-24 00 00", 1379966400l);
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
            expect.append(Integer.valueOf(entry.getKey().split(" ")[2]));

            Assert.assertEquals(expect, result);
        }
    }
}
