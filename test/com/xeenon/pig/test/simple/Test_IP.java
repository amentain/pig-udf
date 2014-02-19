package com.xeenon.pig.test.simple;

import com.xeenon.pig.IP2Long;
import com.xeenon.pig.Long2IP;
import com.xeenon.pig.helper.IPHelper;
import junit.framework.Assert;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Created with IntelliJ IDEA.
 * User: xeenon
 * Date: 13.08.13
 * Time: 9:16
 */
public class Test_IP {
    public Map<String, Long> ips = new HashMap<>();
    public TupleFactory tupleFactory = TupleFactory.getInstance();

    public Test_IP() {
        ips.put("127.0.0.1", 2130706433l);
        ips.put("194.190.116.55", 3267261495l);
        ips.put("194.190.116.194", 3267261634l);
        ips.put("5.19.247.253", 85194749l);
        ips.put("255.255.0.0", 4294901760l);
        ips.put("255.255.255.255", 4294967295l);
    }

    @Test
    public void inet_aton_ntoa() throws Exception {
        for (Map.Entry<String, Long> entry : ips.entrySet()) {
            Assert.assertEquals(entry.getValue(), IPHelper.inet_aton(entry.getKey()));
            Assert.assertEquals(entry.getKey(), IPHelper.inet_ntoa(entry.getValue()));
        }
    }

    @Test
    public void inet_cross() throws Exception {
        for (int i = 0; i < 100; i++) {
            String ip = getRandomIp();
            System.err.println(ip);
            Assert.assertEquals(IPHelper.inet_ntoa(IPHelper.inet_aton(ip)), ip);
        }
    }

    @Test
    public void pig_inet_aton() throws Exception {
        IP2Long ip2Long = new IP2Long();
        for (Map.Entry<String, Long> entry : ips.entrySet()) {
            Tuple input = tupleFactory.newTuple();
            input.append(entry.getKey());

            Assert.assertEquals(entry.getValue(), ip2Long.exec(input));
        }
    }

    @Test
    public void pig_inet_ntoa() throws Exception {
        Long2IP long2Ip = new Long2IP();
        for (Map.Entry<String, Long> entry : ips.entrySet()) {
            Tuple input = tupleFactory.newTuple();
            input.append(entry.getValue());

            Assert.assertEquals(entry.getKey(), long2Ip.exec(input));
        }
    }

    private String getRandomIp() {
        byte[] b = new byte[4];
        new Random().nextBytes(b);

        return String.format(
                "%d.%d.%d.%d",
                b[0] & 0xFF,
                b[1] & 0xFF,
                b[2] & 0xFF,
                b[3] & 0xFF
        );
    }
}
