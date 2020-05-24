package bilian.tech.zhang.hadoop.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

//1.map
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static Logger logger = LoggerFactory.getLogger(WordCountMapper.class.getSimpleName());

    private final static int one = 1;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //获取文件名？》？？
        String path = ((FileSplit)context.getInputSplit()).getPath().toString();
//        FileSplit fileSplit = (FileSplit) context.getInputSplit();

        if (value == null && value.toString().trim().equals("")){

            logger.error("line " + key.get() + " is null");
            return;
        }

        String[] words = value.toString().split(",");
        for (String word : words){

            context.write(new Text(word), new IntWritable(one));
//            logger.info("thread: " + Thread.currentThread().getName() + ", map step ----------> word: " + word + ", count: " + one);
        }
    }
}