package bilian.tech.zhang.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

public class HDFSUtils {

    private static FileSystem getFileSystem() throws IOException {
        Configuration configuration = new Configuration();
        return FileSystem.get(configuration);
    }


    public static int remove(String filePath){

        return 0;
    }
}
