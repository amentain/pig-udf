package com.xeenon.pig.loader;

import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.lang.reflect.Field;
import java.util.LinkedHashMap;

/**
 * Created with IntelliJ IDEA.
 * User: xeenon
 * Date: 03.09.13
 * Time: 12:26
 */
public class XSchema extends LinkedHashMap<String, XSchemaField> {

    public XSchema(Class cls) throws ParseException {
        super(cls.getFields().length);
        for (Field field : cls.getFields()) {
            put(field.getName(), new XSchemaField(field));
        }
    }

    public XSchema(Schema schema) throws ParseException {
        super(schema.size());
        for (Schema.FieldSchema fieldSchema : schema.getFields()) {
            put(fieldSchema.alias, new XSchemaField(fieldSchema));
        }
    }

}
