package com.xeenon.pig;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.data.DataType;
import org.apache.pig.PigException;

public class ParseUnixTimeMilis extends EvalFunc<Tuple> {

    static TupleFactory mTupleFactory = TupleFactory.getInstance();

    public Tuple exec(Tuple input) throws PigException {
        if (input == null || input.size() < 1 || input.get(0) == null)
            return null;

        try {

        // Создаем Date
            java.util.Date dtDate = new java.util.Date((Long) input.get(0));

        // Получаем iso-дату и номер часа
            Tuple output = mTupleFactory.newTuple(2);

                output.set(0, new java.text.SimpleDateFormat("yyyy-MM-dd").format(dtDate));
                output.set(1, new java.text.SimpleDateFormat("H").format(dtDate));

            return output;

        } catch(Exception e){
            System.err.println("Failed to process input; error - " + e.getMessage());
            throw new PigException("Exception in com.kavanga.pig.ParseUnixTimeMilis <"+ input.toString() +"> == ["+ e.getClass().toString() +"] "+ e.toString(), 1300, PigException.INPUT);
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
