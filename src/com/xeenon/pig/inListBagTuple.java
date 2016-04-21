package com.xeenon.pig;

import com.sun.prism.PixelFormat;
import com.xeenon.pig.helper.CacheHelper;
import org.apache.commons.collections.Bag;
import org.apache.pig.FilterFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

public class inListBagTuple extends inList
{

    public inListBagTuple(java.lang.String filterFile) throws IOException {
        super(filterFile);
    }

    public inListBagTuple(java.lang.String ids, java.lang.String separator) throws IOException
    {
        super(ids, separator);
    }

    @Override
    public Boolean exec(Tuple input) throws PigException
    {
        if (input == null || input.isNull(0))
            return null;

        if (input.size() != 2)
            throw new PigException(this.getClass().getCanonicalName() +": wrong param count - must be two", 1300, PigException.INPUT);

        if (input.getType(0) != DataType.BAG)
            throw new PigException(this.getClass().getCanonicalName() +": first param must be Bag of Tuple", 1300, PigException.INPUT);

//        if (input.getType(1) != DataType.CHARARRAY)
//            throw new PigException(this.getClass().getCanonicalName() +": second param must be Chararray", 1300, PigException.INPUT);

        if (idList == null)
            throw new PigException(this.getClass().getCanonicalName() +": get null as idList", PigException.BUG);

        try {
            DataBag bag = (DataBag) input.get(0);
            Integer colId = Integer.valueOf(input.get(1).toString());
//            String colName = input.get(1).toString();

            Iterator<Tuple> iterator = bag.iterator();

            while (iterator.hasNext()) {
                Tuple tuple = iterator.next();

                if (tuple == null || tuple.size() < colId + 1 || tuple.isNull(colId))
                    continue;

                String str = tuple.get(colId).toString();
                if (str.isEmpty())
                    continue;

                if (idList.contains(str))
                    return true;
            }
            return false;

        } catch (ExecException e) {
            System.err.println(this.getClass().getCanonicalName() +": failed to process input; error - " + e.getMessage());
            throw new PigException(this.getClass().getCanonicalName() +": exception in \""+ input.toString() +"\" "+ e.getMessage(), 1300, PigException.INPUT);
        }
    }

    /*
    public Schema outputSchema(Schema inputSchema)
    {
        try {
            if ((inputSchema == null) || ((inputSchema.size() != 1) && (inputSchema.size() != 1))) {
                throw new RuntimeException("Expecting 1 inputs, found: " +
                        ((inputSchema == null) ? 0 : inputSchema.size()));
            }

            Schema.FieldSchema inputFieldSchema = inputSchema.getField(0);
            if (inputFieldSchema.type != DataType.CHARARRAY && inputFieldSchema.type != DataType.LONG) {
                throw new RuntimeException("Expecting a CHARARRAY or LONG, found data type: " +
                        DataType.findTypeName(inputFieldSchema.type));
            }

            return new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY));
        } catch (FrontendException e) {
            e.printStackTrace();
            return null;
        }
    }
    */
}
