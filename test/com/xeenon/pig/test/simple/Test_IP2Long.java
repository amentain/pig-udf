package com.xeenon.pig.test.simple;

import com.xeenon.pig.IP2Long;
import junit.framework.Assert;
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

        ips.put("194.190.116.194", 3267261634l);
        ips.put("5.19.247.253", 85194749l);
        ips.put("255.255.0.0", 4294901760l);
        ips.put("255.255.255.255", 4294967295l);
    }

    @Test
    public void direct() throws Exception {
        for (Map.Entry<String, Long> entry : ips.entrySet()) {
            Assert.assertEquals(entry.getValue(), IP2Long.inet_aton(entry.getKey()));
        }
    }

    @Test
    public void pig() throws Exception {
        IP2Long ip2Long = new IP2Long();
        for (Map.Entry<String, Long> entry : ips.entrySet()) {
            Tuple input = tupleFactory.newTuple();
            input.append(entry.getKey());

            Assert.assertEquals(entry.getValue(), ip2Long.exec(input));
        }
    }
}
