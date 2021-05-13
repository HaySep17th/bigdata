package com.hjc.bigdata.Hadoop.MapReduce.Flowbean.CustomRawComparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @Description : TODO
 * @author : hjc
 * @date : 2021/5/13; 21:02
 * @Version : 1.0
 */
public class FlowbeanDriver {

    public static void main(String[] args) throws Exception {
        Path inputPath = new Path("/Users/apple/Downloads/hadoop/output/flowbean/part-r-00000");
        Path outputPath = new Path("/Users/apple/Downloads/hadoop/output/flowbean/RawComparator");

        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);

        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        Job job = Job.getInstance(conf);
        job.setJobName("RawComparator实现排序");

        job.setMapperClass(FlowbeanMapper.class);
        job.setReducerClass(FlowbeanReducer.class);

        job.setMapOutputKeyClass(Flowbean.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Flowbean.class);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setSortComparatorClass(MyRawComparator.class);

        job.waitForCompletion(true);

    }
}
