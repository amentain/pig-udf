package com.xeenon.pig;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.xeenon.pig.helper.CacheHelper;
import com.xeenon.pig.helper.IPHelper;
import org.apache.commons.lang3.Range;
import org.apache.commons.lang3.StringUtils;
import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.builtin.DoubleAvg;
import org.apache.pig.builtin.FloatAvg;
import org.apache.pig.builtin.IntAvg;
import org.apache.pig.builtin.LongAvg;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * Created with IntelliJ IDEA.
 * User: xeenon
 * Date: 15.10.12
 * Time: 15:32
 */
public class IPFilter extends EvalFunc<String> {
    private static final Pattern ipRange4 = Pattern.compile("^([0-9.]{7,15})([- \t]+([0-9.]{7,15})$)?");

    private HashMap<String, HashSet<Range<Long>>> ipList = new HashMap<>();
    private String cacheFile;

    public IPFilter (String sFilterFile) throws IOException
    {
        cacheFile = CacheHelper.parseCacheFile(sFilterFile);

        FileReader fileReader = new FileReader(cacheFile);
        BufferedReader bufferedReader = new BufferedReader(fileReader);

        Range<Long> range;
        String line, key = null;
        while ((line = StringUtils.trim(bufferedReader.readLine())) != null) {
            if (line.isEmpty())
                continue;

            if ((range = parseRange(line)) == null) {
                key = line;
                continue;
            }
            addRange(key, range);
        }

        bufferedReader.close();
        fileReader.close();
    }

    public IPFilter(String ips, String delim) throws IOException {
        if (ips == null || ips.isEmpty())
            throw new IOException("no ips passed in constructor");

        if (delim == null || delim.isEmpty())
            throw new IOException("no delimiter passed in constructor");

        String key = null;
        Range<Long> range;
        for (String line : StringUtils.split(ips, delim)) {
            if ((range = parseRange(line)) == null) {
                key = line;
                continue;
            }
            addRange(key, range);
        }
    }

    @Override
    public List<String> getCacheFiles()
    {
        if (cacheFile == null)
            return null;

        return Arrays.asList(cacheFile);
    }

    @Override
    public String exec(Tuple input) throws IOException
    {
        if (input == null || input.get(0) == null)
            return null;

        if (input.size() > 1)
            throw new PigException(this.getClass().getCanonicalName() +": wrong param count - must be one", 1300, PigException.INPUT);

        if (ipList == null)
            throw new PigException(this.getClass().getCanonicalName() +": got null as idList");

        if (input.getType(0) == DataType.LONG)
            return matches((Long) input.get(0));

        return matches(IPHelper.inet_aton(input.get(0).toString()));
    }

    public String matches(Long ip) {
        for (String key : ipList.keySet()) {
            for (Range<Long> range : ipList.get(key)) {
                if (range.contains(ip))
                    return key;
            }
        }
        return null;
    }

    public void addRange(String key, Range<Long> range) {
        if (key == null || key.isEmpty())
            key = "ip";

        if (!ipList.containsKey(key))
            ipList.put(key, new HashSet<Range<Long>>());

        ipList.get(key).add(range);
    }

    public Range<Long> parseRange(String line) {
        Matcher matcher = ipRange4.matcher(line);
        if (!matcher.matches())
            return null;

        if (matcher.group(3) != null)
            return Range.between(
                    IPHelper.inet_aton(matcher.group(1)),
                    IPHelper.inet_aton(matcher.group(3))
            );
        else {
            Long ip = IPHelper.inet_aton(matcher.group(1));
            return Range.between(ip, ip);
        }
    }

    @Override
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
}

