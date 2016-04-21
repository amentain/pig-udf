package com.xeenon.pig;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import java.util.HashSet;
import java.util.List;
import java.util.Arrays;

import com.xeenon.pig.helper.CacheHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.FilterFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.UDFContext;

public class inList extends FilterFunc
{
    protected final HashSet<String> idList = new HashSet<>();
    private java.lang.String cacheFile;
    private final boolean fromHDFS;

    public inList(java.lang.String filterFile) throws IOException
    {
        if (filterFile.startsWith("hdfs|")) {
            fromHDFS = true;
            cacheFile = CacheHelper.parseCacheFile(filterFile.replace("hdfs|", ""));
        } else {
            fromHDFS = false;
            cacheFile = CacheHelper.parseCacheFile(filterFile);
        }

        System.err.println(String.format("[com.xeenon.pig.inList] sFilterFile: %s | cacheFile: %s", filterFile, cacheFile));

        File checkFile = new File(cacheFile);
        if (!checkFile.exists() || !checkFile.isFile())
            return;

        FileReader fileReader = new FileReader(cacheFile);
        BufferedReader bufferedReader = new BufferedReader(fileReader);

        java.lang.String line;
        while ((line = bufferedReader.readLine()) != null) {
            idList.add(line);
        }

        bufferedReader.close();
        fileReader.close();
    }

    public inList(java.lang.String ids, java.lang.String separator) throws IOException
    {
        if (ids == null)
            throw new IOException(this.getClass().getCanonicalName() +": got null as ids");

        if (separator == null)
            throw new IOException(this.getClass().getCanonicalName() +": got null as separator");

        fromHDFS = false;
        for (java.lang.String id : ids.split(separator)) {
            idList.add(id);
        }
    }

    @Override
    public List<String> getCacheFiles() {
        if (!fromHDFS || cacheFile == null)
            return null;

        System.err.println(String.format("Cache: %s", cacheFile));
        return Arrays.asList(cacheFile);
    }

    @Override
    public List<String> getShipFiles() // public List<java.lang.String> getCacheFiles()
    {
        if (fromHDFS || cacheFile == null)
            return null;

        System.err.println(String.format("Ship: %s", cacheFile));
        return Arrays.asList(cacheFile);
    }

    @Override
    public Boolean exec(Tuple input) throws PigException
    {
        if (input == null || input.get(0) == null)
            return null;

        if (input.size() > 1)
            throw new PigException(this.getClass().getCanonicalName() +": wrong param count - must be one", 1300, PigException.INPUT);

        if (idList == null)
            throw new PigException(this.getClass().getCanonicalName() +": get null as idList", PigException.BUG);

        try {
            java.lang.String str = input.get(0).toString();
            if (str.isEmpty())
                return null;

            return idList.contains(str);

        } catch (ExecException e) {
            System.err.println(this.getClass().getCanonicalName() +": failed to process input; error - " + e.getMessage());
            throw new PigException(this.getClass().getCanonicalName() +": exception in \""+ input.toString() +"\" "+ e.getMessage(), 1300, PigException.INPUT);
        }
    }
}
