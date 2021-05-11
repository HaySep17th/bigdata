package com.hjc.bigdata.Hadoop.MapReduce.FlowbeanSort2;

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
 * @date : 2021/5/11; 21:22
 * @Version : 1.0
 */
public class FlowbeanSort2Driver {

    public static void main(String[] args) throws Exception{

        Path inputPath = new Path ("/Users/apple/Downloads/hadoop/output/flowbean/part-r-00000");
        Path outputPath = new Path ("/Users/apple/Downloads/hadoop/output/flowbeanSort2");

        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);

        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        Job job = Job.getInstance(conf);
        job.setJobName("FlowbeanSort2");

        job.setMapperClass(FlowbeanSort2Mapper.class);
        job.setReducerClass(FlowbeanSort2Reducer.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setSortComparatorClass(MyComparator.class);

        job.waitForCompletion(true);

    }

}
