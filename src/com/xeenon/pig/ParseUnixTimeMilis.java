package com.xeenon.pig;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.data.DataType;
import org.apache.pig.PigException;

import java.text.SimpleDateFormat;

public class ParseUnixTimeMilis extends EvalFunc<Tuple> {
    private static final TupleFactory mTupleFactory = TupleFactory.getInstance();
    private static final SimpleDateFormat date = new java.text.SimpleDateFormat("yyyy-MM-dd");
    private static final SimpleDateFormat hour = new java.text.SimpleDateFormat("H");

    public Tuple exec(Tuple input) throws PigException {
        if (input == null || input.size() < 1 || input.isNull(0))
            return null;

        try {
            java.util.Date dtDate = new java.util.Date((Long) input.get(0));

            Tuple output = mTupleFactory.newTuple();
            output.append(date.format(dtDate));
            output.append(hour.format(dtDate));

            return output;
        } catch(Exception e){
            System.err.println("Failed to process input; error - " + e.getMessage());
            throw new PigException("Exception in com.xeenon.pig.ParseUnixTimeMilis <"+ input.toString() +"> == ["+ e.getClass().toString() +"] "+ e.toString(), 1300, PigException.INPUT);
        }
    }

    public Schema outputSchema(Schema input) {
        try {
            Schema parser = new Schema();
            parser.add(new Schema.FieldSchema("date", DataType.CHARARRAY));
            parser.add(new Schema.FieldSchema("hour", DataType.INTEGER));

            return new Schema(new Schema.FieldSchema("dt", parser, DataType.TUPLE));
        } catch (Exception e) {
            return null;
        }
    }
}
