package com.xeenon.pig.loader;

import org.apache.commons.lang3.StringUtils;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;

import java.lang.reflect.Method;
import java.util.ArrayList;

/**
 * Created with IntelliJ IDEA.
 * User: xeenon
 * Date: 03.09.13
 * Time: 11:07
 */
public class Array<T> extends ArrayList<T> {

    public Array() {
        super();
    }

    public Array(Integer size) {
        super(size);
    }

    public static Array fromString(Class cls, String line, String delim) throws ParseException {
        if (line == null || delim == null)
            return null;

        try {
            Array<Object> res = new Array<>();
            if (cls.equals(String.class)) {
                for (String item : StringUtils.split(line, delim)) {
                    res.add(item);
                }
            } else {
                Method valueOf = cls.getMethod("valueOf", String.class);
                for (String item : StringUtils.split(line, delim)) {
                    res.add(valueOf.invoke(null, item));
                }
            }
            return res;
        } catch (NoSuchMethodException e) {
            throw new ParseException(String.format(
                    "can't find %s.valueOf(String str)",
                    cls.getName()
            ), e);
        } catch (Throwable e) {
            throw new ParseException(e);
        }
    }

    public static Array fromJson(Class cls, JsonParser p) throws ParseException {
        if (p.getCurrentToken() != JsonToken.START_ARRAY)
            throw new ParseException(String.format("START_ARRAY expected, but %s found", p.getCurrentToken().toString()));

        try {
            JsonToken t;
            Array<Object> res = new Array<>();
            Method valueOf = cls.getMethod("valueOf", String.class);
            while ((t = p.nextToken()) != JsonToken.END_ARRAY) {
                if (t == null)
                    throw new ParseException("early termination");

                res.add(valueOf.invoke(null, p.getText()));
            }
            return res;
        } catch (NoSuchMethodException e) {
            throw new ParseException(String.format(
                    "can't find %s.valueOf(String str)",
                    cls.getName()
            ), e);
        } catch (Throwable e) {
            throw new ParseException(e);
        }
    }

    public static Array fromBag(Class cls, DataBag bag) throws ParseException {
        if (bag == null)
            return null;

        try {
            Array<Object> res = new Array<>();
            Method valueOf = cls.getMethod("valueOf", String.class);
            for (Tuple tuple : bag) {
                if (tuple == null || tuple.size() == 0 || tuple.isNull(0))
                    res.add(null);

                if (tuple.size() != 1)
                    throw new ParseException(String.format("size of BAG's inner TUPLE must be <= 1, but %d found", tuple.size()));

                res.add(valueOf.invoke(null, tuple.get(0).toString()));
            }
            return res;
        } catch (NoSuchMethodException e) {
            throw new ParseException(String.format(
                    "can't find %s.valueOf(String str)",
                    cls.getName()
            ), e);
        } catch (Throwable e) {
            throw new ParseException(e);
        }
    }
}
