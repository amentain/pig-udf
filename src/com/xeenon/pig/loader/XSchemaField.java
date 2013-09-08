package com.xeenon.pig.loader;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

/**
 * Created with IntelliJ IDEA.
 * User: xeenon
 * Date: 03.09.13
 * Time: 11:06
 */
public class XSchemaField {
    private String fieldName;
    private Class fieldClass;
    private Method valueOf;

    private XSchema schema = null;

    public XSchemaField(Field field) throws ParseException {
        fieldName = field.getName();
        fieldClass = field.getClass();

        if (fieldClass.getFields().length > 0) {
            schema = new XSchema(fieldClass);
        }
    }

    public XSchemaField(Schema.FieldSchema fieldSchema) throws ParseException {
        fieldName = fieldSchema.alias;
        switch (fieldSchema.type) {
            case DataType.BAG:
            case DataType.TUPLE:
                throw new ParseException("not implemented");

            case DataType.BOOLEAN:
                fieldClass = Boolean.class;
                break;

            case DataType.BYTE:
                fieldClass = Byte.class;

            case DataType.INTEGER:
                fieldClass = Integer.class;
                break;

            case DataType.LONG:
                fieldClass = Long.class;
                break;

            case DataType.FLOAT:
                fieldClass = Float.class;
                break;

            case DataType.DOUBLE:
                fieldClass = Double.class;
                break;

            case DataType.DATETIME:
                throw new ParseException("not implemented");

            case DataType.BYTEARRAY:
                fieldClass = Byte[].class;
                break;

            case DataType.CHARARRAY:
            case DataType.BIGCHARARRAY:
                fieldClass = String.class;
                break;

            default:
                throw new ParseException("unknown data type");
        }

        try {
            valueOf = fieldClass.getMethod("valueOf", String.class);
        } catch (NoSuchMethodException e) {
            throw new ParseException("can't find valueOf(String)", e);
        }
    }

    public Object valueOf() {
        return new Object();
    }

    public String getFieldName() {
        return fieldName;
    }

    public Class getFieldClass() {
        return fieldClass;
    }

    public XSchema getFieldSchema() {
        return schema;
    }

}
