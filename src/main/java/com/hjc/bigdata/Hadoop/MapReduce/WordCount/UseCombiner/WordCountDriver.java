package com.hjc.bigdata.Hadoop.MapReduce.WordCount.UseCombiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @Description : TODO
 * @author : hjc
 * @date : 2021/5/14; 20:14
 * @Version : 1.0
 */
public class WordCountDriver {

    public static void main(String[] args) throws Exception{

        Path inputPath = new Path("/Users/apple/Downloads/hadoop/mrinput/wordcount/HarryPotter.txt");
        Path outputPath = new Path("/Users/apple/Downloads/hadoop/output/wordcount/UseCombiner");

        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);

        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        Job job = Job.getInstance(conf);
        job.setJobName("使用combiner之后的wordcount");

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        /**
         * 使用Combiner之前：
         *  Map output records=8084282；Map output materialized bytes=93127928；Reduce shuffle bytes=93127928
         *  使用Combiner之后：
         *  Map output records=410616； Map output materialized bytes=466722；  Reduce shuffle bytes=466722
         */
        job.setCombinerClass(WordCountReducer.class);

        job.waitForCompletion(true);
    }
}
