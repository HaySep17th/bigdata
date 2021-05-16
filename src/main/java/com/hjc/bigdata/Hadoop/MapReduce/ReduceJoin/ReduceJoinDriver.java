package com.hjc.bigdata.Hadoop.MapReduce.ReduceJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @Description : TODO
 * @author : hjc
 * @date : 2021/5/16; 15:18
 * @Version : 1.0
 */
public class ReduceJoinDriver {

    public static void main(String[] args) throws Exception {
        Path inputPath = new Path("/Users/apple/Downloads/hadoop/mrinput/reducejoin");
        Path outputPath = new Path("/Users/apple/Downloads/hadoop/output/reducejoin");

        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);

        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        Job job = Job.getInstance(conf);
        job.setJobName("Reducer实现join");

        job.setMapperClass(ReducerJoinMapper.class);
        job.setReducerClass(ReducerJoinReducer.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(JoinBean.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setPartitionerClass(ReduceJoinPartitioner.class);

        job.waitForCompletion(true);
    }
}
