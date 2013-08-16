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
public class RecordReader extends org.apache.hadoop.mapreduce.RecordReader {
    private String[] lines;
    private int pos = -1;

    public RecordReader(String[] lines) {
        super();
        this.lines = lines;
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
        return new Text(lines[pos].getBytes(Charset.forName("UTF-8")));
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
