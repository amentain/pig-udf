package com.xeenon.pig.loader;

/**
 * Created with IntelliJ IDEA.
 * User: xeenon
 * Date: 03.09.13
 * Time: 11:44
 */
public class ParseException extends Exception {

    public ParseException(String msg) {
        super(msg);
    }

    public ParseException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public ParseException(Throwable cause) {
        super(cause);
    }
}
