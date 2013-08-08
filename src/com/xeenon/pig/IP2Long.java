package com.xeenon.pig;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigException;
import org.apache.pig.data.Tuple;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: xeenon
 * Date: 10.10.12
 * Time: 18:26
 */
public class IP2Long extends EvalFunc<Long> {

    @Override
    public Long exec(Tuple input) throws IOException {
        if (input == null || input.size() != 1 || input.isNull(0))
            return null;

        try {
            return inet_aton(input.get(0).toString());
        } catch(Exception e) {
            String msg = String.format(
                    "%s got %s on \"%s\"",
                    this.getClass().getCanonicalName(),
                    e.getMessage(),
                    input.toString()
            );
            System.err.println(msg);
            throw new PigException(msg, e);
        }
    }

    public static Long inet_aton(String dottedIP)
    {
        String[] addr = dottedIP.split("\\.");

        long num = 0;
        for (int i = 0; i < addr.length; i++) {
            int power = 3 - i;
            num += ((Integer.parseInt(addr[i]) % 256) * Math.pow(256, power));
        }

        return num;
    }
}
