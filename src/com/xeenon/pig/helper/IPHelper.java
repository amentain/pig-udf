package com.xeenon.pig.helper;

import org.apache.commons.lang3.StringUtils;

/**
 * Created with IntelliJ IDEA.
 * User: xeenon
 * Date: 19.02.14
 * Time: 16:20
 */
public class IPHelper {

    public static Long inet_aton(String dottedIP)
    {
        if (dottedIP == null)
            return null;

        long num = 0;
        String[] addr = StringUtils.split(dottedIP, '.');
        for (int i = 0; i < addr.length; i++) {
            int power = 3 - i;
            num += ((Integer.parseInt(addr[i]) % 256) * Math.pow(256, power));
        }

        return num;
    }

    public static String inet_ntoa(Long ip) {
        if (ip == null)
            return null;

        return String.format(
                "%d.%d.%d.%d",
                ((  ip >> 24 )  & 0xFF),
                ((  ip >> 16 )  & 0xFF),
                ((  ip >> 8  )  & 0xFF),
                (   ip          & 0xFF)
        );
    }
}
