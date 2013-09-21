package com.xeenon.pig;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.pig.PigWarning;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;

/**
 * Created with IntelliJ IDEA.
 * User: xeenon
 * Date: 16.09.13
 * Time: 14:55
 */
public class JsonLoader extends org.apache.pig.builtin.JsonLoader {
  private JsonFactory jsonFactory = new JsonFactory();
  private TupleFactory tupleFactory = TupleFactory.getInstance();
  private BagFactory bagFactory = BagFactory.getInstance();

  public JsonLoader(String schemaString) throws IOException {
      super(schemaString);
  }

  @Override
  public Tuple getNext() throws IOException {
      Text val;
      try {
          // Read the next key value pair from the record reader.  If it's
          // finished, return null
          if (!reader.nextKeyValue()) return null;

          // Get the current value.  We don't use the key.
          val = (Text)reader.getCurrentValue();
      } catch (InterruptedException ie) {
          throw new IOException(ie);
      }

      // Create a parser specific for this input line.  This may not be the
      // most efficient approach.
      byte[] newBytes = new byte[val.getLength()];
      System.arraycopy(val.getBytes(), 0, newBytes, 0, val.getLength());
      ByteArrayInputStream bais = new ByteArrayInputStream(newBytes);
      JsonParser p = jsonFactory.createJsonParser(bais);

  //    try {
  //        RepublerLog log = RepublerLog.fromJson(p);
  //        return log == null ? null : log.toTuple();
  //    } catch (LogException e) {
   //       e.printStackTrace();
   //       warn(e.getMessage(), PigWarning.UDF_WARNING_1);
          return null;
   //   }
  }

  public static void main(String[] args)
  {
      //try {
          RecordReader<Text, Text> reader = new RecordReader<Text, Text>() {

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
                  Text text = new Text("{\"site_id\":4020,\"page_id\":3354,\"siteIab\":[\"IAB2\"],\"pageIab\":[\"IAB2\"],\"kvng_cat_id\":\"1\",\"blind\":false,\"geo_id\":0,\"uid\":\"7045072540187634716\",\"ip\":\"194.190.116.3\",\"ts\":1376735573123,\"ua\":\"Mozilla\\/5.0 (Windows NT 6.1; WOW64; rv:22.0) Gecko\\/20100101 Firefox\\/22.0 FirePHP\\/0.7.2\",\"page_url\":\"\\/exp?sid=4020&bt=5&bn=1&bc=3&ct=1&pr=90719&pt=b&pd=17&pw=6&pv=14&prr=\",\"page_referer\":\"http:\\/\\/jl.nicklosev.com\\/4020.html\",\"time\":123,\"impressions\":[{\"id\":\"1\",\"place_id\":37008,\"type_id\":5,\"dsp_id\":1,\"frame_id\":2,\"floorPrice\":10.0,\"iprice\":12.6,\"oprice\":11.351351205060917,\"imoney\":0.0126,\"omoney\":0.011351351205060917,\"currency\":\"\"}],\"responses\":[{\"dsp_id\":1,\"frame_id\":2,\"time\":35,\"seats\":[{\"grouped\":false,\"bids\":[{\"impid\":1,\"price\":15.0,\"currency\":\"\"}]}]},{\"dsp_id\":32,\"frame_id\":104,\"time\":41,\"seats\":[{\"grouped\":false,\"bids\":[{\"impid\":1,\"price\":12.5,\"currency\":\"RUB\"}]}]}],\"outsiders\":[]}".getBytes(Charset.forName("UTF-8")));

                  return text;
              }

              @Override
              public float getProgress() throws IOException, InterruptedException {
                  return first ? 0 : 100;
              }

              @Override
              public void close() throws IOException {

              }
          };

          /*
          RepublerLoader loader = new RepublerLoader("site_id:int,page_id:int,siteIab:{(iab:chararray)},pageIab:{(iab:chararray)},kvng_cat_id:chararray,blind:chararray,geo_id:int,uid:chararray,ip:chararray,ts:long,ua:chararray,page_url:chararray,page_referer:chararray,time:int,impressions:{(id:chararray,place_id:int,type_id:int,bid_id:chararray,ad_id:chararray,dsp_id:int,frame_id:int,floorPrice:double,iprice:double,oprice:double,imoney:double,omoney:double,currency:chararray)},responses:{(dsp_id:int,frame_id:int,time:int,seats:{(grouped:chararray,bids:{(id:chararray,ad_id:chararray,impid:chararray,price:double,currency:chararray)})})},outsiders:{(impid:chararray,dsp_id:int,frame_id:int)}");

          loader.getSchema(null, null);
          loader.prepareToRead(reader, null);

          Tuple tuple = loader.getNext();

          Tuple input = TupleFactory.getInstance().newTuple();
          input.append(tuple.get(14));
          input.append(tuple.get(15));
          input.append(tuple.get(16));

          LogCombiner logCombiner = new LogCombiner();
          Tuple result = logCombiner.exec(input);

          System.err.println(tuple == null ? "NULL" : tuple.toString());
          System.err.println(result == null ? "NULL" : result.toString());

          */
    //  } catch (IOException e) {
    //      e.printStackTrace();
    //  }
  }

}
