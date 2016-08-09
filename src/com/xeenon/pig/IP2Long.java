package com.xeenon.pig;

import com.xeenon.pig.helper.IPHelper;
import org.apache.commons.lang3.StringUtils;
import org.apache.pig.EvalFunc;
import org.apache.pig.PigException;
import org.apache.pig.PigWarning;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

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
        if (input == null || input.size() == 0 || input.isNull(0))
            return null;

        try {
            return IPHelper.inet_aton(input.get(0).toString());
        } catch(Exception e) {
            String msg = String.format(
                    "%s got %s on \"%s\"",
                    this.getClass().getCanonicalName(),
                    e.getMessage(),
                    input.toString()
            );
            System.err.println(msg);
            warn(msg, PigWarning.UDF_WARNING_1);
            return null;
        }
    }

    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.LONG));
    }

    @Override
    public boolean allowCompileTimeCalculation() {
        return true;
    }
}
