package com.xeenon.pig;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import java.util.HashSet;
import java.util.List;
import java.util.Arrays;

import org.apache.pig.FilterFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

import org.apache.pig.impl.util.UDFContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;

import java.util.regex.Pattern;

public class inList extends FilterFunc
{
    private HashSet<String> idList;
    private String cacheFile;

    // TODO: move load to separate class
    public inList (String sFilterFile) throws IOException
    {
        idList = new HashSet<>();
        cacheFile = sFilterFile;
        if (sFilterFile == null) {
            throw new IOException(this.getClass().getCanonicalName() +": get null as sFilterFile");
        }

        Configuration conf = UDFContext.getUDFContext().getJobConf();
        if (conf == null) {
        // running on frontend (pig plan builder?)
            return;
        }

        File inFile = new File(sFilterFile);
        try {
        // if NullPointerException => Local Mode
            Path[] localCache = DistributedCache.getLocalCacheFiles(conf);

            Pattern p = Pattern.compile(".*?"+ Pattern.quote(inFile.getName()) + "$");
            for (Path localFile : localCache) {
                if (p.matcher(localFile.getName()).matches()) {
                    loadCache(localFile.toUri().getPath());
                    return;
                }
            }
        } catch (NullPointerException e) {
        // Local mode possible
            if (inFile.exists()) {
                loadCache(inFile.getPath());
                return;
            }

            throw new IOException(this.getClass().getCanonicalName() +": can't find \""+ inFile.getPath() +"\" in LocalStorage");
        }

        throw new IOException(this.getClass().getCanonicalName() +": can't find \""+ inFile.getName() +"\" in DistributedCache");
    }

    public inList (String ids, String separator) throws IOException
    {
        idList = new HashSet<>();
        if (ids == null)
            throw new IOException(this.getClass().getCanonicalName() +": got null as ids");

        if (separator == null)
            throw new IOException(this.getClass().getCanonicalName() +": got null as separator");

        for (String id : ids.split(separator)) {
            idList.add(id);
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
    public Boolean exec(Tuple input) throws PigException
    {
        if (input == null || input.get(0) == null)
            return null;

        if (input.size() > 1)
            throw new PigException(this.getClass().getCanonicalName() +": wrong param count - must be one", 1300, PigException.INPUT);

        if (idList == null)
            throw new PigException(this.getClass().getCanonicalName() +": get null as idList");

        try {
            String str = input.get(0).toString();
            if (str.isEmpty())
                return null;

            return idList.contains(str);

        } catch (ExecException e) {
            System.err.println(this.getClass().getCanonicalName() +": failed to process input; error - " + e.getMessage());
            throw new PigException(this.getClass().getCanonicalName() +": exception in \""+ input.toString() +"\" "+ e.getMessage(), 1300, PigException.INPUT);
        }
    }

    private void loadCache(String fileName) throws IOException
    {
        FileReader fileReader = new FileReader(fileName);
        BufferedReader bufferedReader = new BufferedReader(fileReader);

        String line;
        while ((line = bufferedReader.readLine()) != null) {
            idList.add(line);
        }

        bufferedReader.close();
        fileReader.close();
    }
}
