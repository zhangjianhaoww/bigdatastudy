package bilian.tech.zhang.hadoop.mr.wordcount;

import bilian.tech.zhang.hadoop.mr.WordCountMR;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

//2.reduce
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private static Logger logger = LoggerFactory.getLogger(WordCountReducer.class.getSimpleName());

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int count = 0;

        for (IntWritable value : values){
            count = count + value.get();
        }

        context.write(key, new IntWritable(count));
//        logger.info("thread: " + Thread.currentThread().getName() + ", reduce step ----------> word: " + key.toString() + ", count: " + count);
    }
}
