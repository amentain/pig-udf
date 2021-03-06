package com.xeenon.pig.helper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.pig.impl.util.UDFContext;

import java.io.File;
import java.io.IOException;
import java.util.regex.Pattern;

/**
 * Created with IntelliJ IDEA.
 * User: xeenon
 * Date: 22.08.13
 * Time: 21:16
 */
public class CacheHelper {

    public static String parseCacheFile (String sFilterFile) throws IOException
    {
        if (sFilterFile == null) {
            throw new IOException("CacheHelper: get null as sFilterFile");
        }

        if (UDFContext.getUDFContext().isFrontend()) // running on frontend (pig plan builder?)
            return sFilterFile;

        File inFile = new File(sFilterFile);
        if (inFile.exists()) {
            return sFilterFile; // localMode ?
        }

        inFile = new File(String.format("./%s", inFile.getName()));
        if (inFile.exists()) {
            return inFile.getAbsolutePath();
        }

        try {
            // TODO: fix for local mode
            Configuration conf = UDFContext.getUDFContext().getJobConf();
            Path[] localCache = DistributedCache.getLocalCacheFiles(conf);

            Pattern p = Pattern.compile(".*?"+ Pattern.quote(inFile.getName()) + "$");
            for (Path localFile : localCache) {
                System.err.println("Found in DS:"+ localFile.toUri().getPath());
                if (p.matcher(localFile.getName()).matches())
                    return localFile.toUri().getPath();
            }
            throw new IOException(String.format(
                    "%s: can't find \"%s\" in DistributedCache",
                    "CacheHelper",
                    inFile.getPath()
            ));
        } catch (NullPointerException e) {
            // Local mode possible
            throw new IOException(String.format(
                    "%s: can't find \"%s\" in LocalStorage",
                    "CacheHelper",
                    inFile.getPath()
            ));
        }
    }
}
