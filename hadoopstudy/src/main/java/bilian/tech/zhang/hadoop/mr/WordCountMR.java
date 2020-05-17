package bilian.tech.zhang.hadoop.mr;

import bilian.tech.zhang.hadoop.mr.wordcount.WordCountMapper;
import bilian.tech.zhang.hadoop.mr.wordcount.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class WordCountMR extends Configured implements Tool {

    private static Logger logger = LoggerFactory.getLogger(WordCountMR.class.getSimpleName());

    @Override
    public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //1 get conf
        Configuration configuration = getConf();
        System.setProperty("HADOOP_USER_NAME", "haru");

        //2 create job
        Job job = Job.getInstance(configuration, this.getClass().getSimpleName());
        job.setJarByClass(WordCountMR.class);

        //3.1 input
        Path inputPath = new Path(args[0]);
        FileInputFormat.addInputPath(job, inputPath);

        //3.2 map
        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //3.3 reduce
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //3.4 output
        Path outPath = new Path(args[1]);

        FileSystem fileSystem = FileSystem.get(configuration);
        if (fileSystem.exists(outPath)){
            fileSystem.delete(outPath, true);
        }
        FileOutputFormat.setOutputPath(job, outPath);

        //4 commit
        boolean isSuccess = job.waitForCompletion(true);

        return isSuccess ? 1 : 0;
    }


    public static void main(String[] args) {



        WordCountMR wordCountMR = new WordCountMR();
        Configuration configuration = new Configuration();

        int status = 0;

            try {
                status = ToolRunner.run(configuration, wordCountMR, args);
                System.exit(status);
            }catch (Exception e){
                e.printStackTrace();
            }


    }


}
