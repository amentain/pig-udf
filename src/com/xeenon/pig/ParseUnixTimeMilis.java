package com.xeenon.pig;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.data.DataType;
import org.apache.pig.PigException;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class ParseUnixTimeMilis extends EvalFunc<Tuple> {
    private static final TupleFactory mTupleFactory = TupleFactory.getInstance();
    private static final DateTimeFormatter dateFormat = DateTimeFormat.forPattern("yyyy-MM-dd");

    public Tuple exec(Tuple input) throws PigException {
        if (input == null || input.size() < 1 || input.isNull(0))
            return null;

        try {
            DateTime dt = new DateTime(input.get(0));

            Tuple output = mTupleFactory.newTuple(3);
            output.set(0, dateFormat.print(dt));
            output.set(1, dt.getHourOfDay());
            output.set(2, dt.getMinuteOfHour());

            return output;
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

    public Schema outputSchema(Schema input) {
        try {
            Schema parser = new Schema();
            parser.add(new Schema.FieldSchema("date", DataType.CHARARRAY));
            parser.add(new Schema.FieldSchema("hour", DataType.INTEGER));
            parser.add(new Schema.FieldSchema("minute", DataType.INTEGER));

            return new Schema(new Schema.FieldSchema("dt", parser, DataType.TUPLE));
        } catch (Exception e) {
            return null;
        }
    }
}
