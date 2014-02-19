package com.xeenon.pig;

import com.xeenon.pig.helper.IPHelper;
import org.apache.pig.EvalFunc;
import org.apache.pig.PigException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: xeenon
 * Date: 19.02.14
 * Time: 15:33
 */
public class Long2IP extends EvalFunc<String> {

    @Override
    public String exec(Tuple input) throws IOException {
        if (input == null || input.size() != 1 || input.isNull(0) || input.getType(0) != DataType.LONG)
            return null;

        try {
            return IPHelper.inet_ntoa(Long.valueOf(input.get(0).toString()));
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
}
