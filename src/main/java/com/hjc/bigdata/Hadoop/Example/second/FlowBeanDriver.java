package com.hjc.bigdata.Hadoop.Example.second;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @Description : TODO
 * @author : hjc
 * @date : 2021/5/17; 21:20
 * @Version : 1.0
 */
public class FlowBeanDriver {

    public static void main(String[] args) throws Exception {
        Path inputPath = new Path("/Users/apple/Downloads/hadoop/mrinput/topN");
        Path outputPath = new Path("/Users/apple/Downloads/hadoop/output/topN");

        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        Job job = Job.getInstance(conf);

        job.setMapperClass(FlowBeanMapper.class);
        job.setReducerClass(FlowBeanReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setSortComparatorClass(LongWritable.DecreasingComparator.class);

        job.waitForCompletion(true);
    }
}
