package bilian.tech.zhang.hadoop.mr.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class DistributedCacheMR {

    public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        Map<String, String> customerMap = new HashMap<>();

        /**
         * 将数据集量小的数据在setup阶段写入到内存 Map() 中。
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            URI[] uris = Job.getInstance(conf).getCacheFiles();

            Path path = new Path(uris[0]);

            FileSystem fs = FileSystem.get(conf);
            InputStream inputStream = fs.open(path);

            InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

            String line = null;
            while ((line = bufferedReader.readLine()) != null){
                if (line.trim().length() > 0){
                    customerMap.put(line.split(",")[0], line);
                }
            }

            bufferedReader.close();
            inputStreamReader.close();
            inputStream.close();
            fs.close();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            super.map(key, value, context);
        }


    }


    public static void main(String[] args) {

        
    }

}
