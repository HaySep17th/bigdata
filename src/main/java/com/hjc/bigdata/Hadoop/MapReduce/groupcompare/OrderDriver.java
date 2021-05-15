package com.hjc.bigdata.Hadoop.MapReduce.groupcompare;

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
 * @date : 2021/5/15; 21:53
 * @Version : 1.0
 */
public class OrderDriver {

    public static void main(String[] args) throws Exception{

        Path inputPath = new Path("/Users/apple/Downloads/hadoop/mrinput/groupcomparator/data.txt");
        Path outputPath = new Path("/Users/apple/Downloads/hadoop/output/groupcomparator/");

        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);

        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        Job job = Job.getInstance(conf);
        job.setJobName("分组");

        job.setMapperClass(OrderMapper.class);
        job.setReducerClass(OrderReducer.class);

        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setGroupingComparatorClass(MyComparator.class);

        job.waitForCompletion(true);

    }

}
