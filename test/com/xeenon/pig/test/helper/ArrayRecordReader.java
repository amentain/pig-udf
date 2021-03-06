package com.xeenon.pig.test.helper;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.nio.charset.Charset;

/**
 * Created with IntelliJ IDEA.
 * User: xeenon
 * Date: 13.08.13
 * Time: 9:15
 */
public class ArrayRecordReader extends org.apache.hadoop.mapreduce.RecordReader {
    private Text[] lines;
    private int pos = -1;

    public ArrayRecordReader(String[] lines) {
        super();

        this.lines = new Text[lines.length];
        for (int i = 0; i < lines.length; i++) {
            this.lines[i] = new Text(lines[i].getBytes(Charset.forName("UTF-8")));
        }
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return pos++ > lines.length;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return null;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return lines[pos];
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (pos == 0)
            return 0;

        return lines.length / pos;
    }

    @Override
    public void close() throws IOException {

    }
}
