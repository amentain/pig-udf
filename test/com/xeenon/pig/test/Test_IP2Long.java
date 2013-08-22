package com.xeenon.pig.test;

import com.xeenon.pig.IP2Long;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: xeenon
 * Date: 13.08.13
 * Time: 9:16
 */
public class Test_IP2Long {
    public Map<String, Long> ips = new HashMap<>();
    public TupleFactory tupleFactory = TupleFactory.getInstance();

    public Test_IP2Long() {
        ips.put("127.0.0.1", 2130706433l);
        ips.put("194.190.116.55", 3267261495l);
        ips.put("5.19.247.253", 85194749l);
    }

    @Test
    public void pig() throws Exception {

        IP2Long ip2Long = new IP2Long();
        for (Map.Entry<String, Long> entry : ips.entrySet()) {
            Tuple input = tupleFactory.newTuple(1);
            input.set(0, entry.getKey());

            Long ipn = ip2Long.exec(input);
            if (!ipn.equals(entry.getValue())) {
                throw new Exception("wrong ip conversion");
            }
        }
    }
}
