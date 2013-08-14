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
    Boolean first = true;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (first) {
            first = !first;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return null;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        Text text = new Text("{\"site_id\":4334,\"page_id\":3779,\"geo_id\":0,\"uid\":\"7159008078565156977\",\"ip\":\"10.8.1.11\",\"ts\":1375869842741,\"page_url\":\"/exp?sid=4334&bt=5&bn=1&bc=3&ct=2&pr=3686&pt=b&pd=24&pw=3&pv=18&prr=\",\"page_referer\":\"\",\"time\":105,\"impressions\":[{\"id\":\"1\",\"place_id\":38454,\"type_id\":5,\"dsp_id\":0,\"frame_id\":0,\"floorPrice\":5.0,\"iprice\":0.0,\"oprice\":0.0,\"imoney\":0.0,\"omoney\":0.0,\"currency\":\"\"}],\"responses\":[],\"outsiders\":[{\"impid\":\"1\",\"dsp_id\":32,\"frame_id\":104}]}".getBytes(Charset.forName("UTF-8")));

        return text;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return first ? 0 : 100;
    }

    @Override
    public void close() throws IOException {

    }
}
