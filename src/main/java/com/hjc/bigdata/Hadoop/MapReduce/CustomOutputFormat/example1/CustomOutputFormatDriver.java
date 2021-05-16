package com.hjc.bigdata.Hadoop.MapReduce.CustomOutputFormat.example1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @Description : TODO
 * @author : hjc
 * @date : 2021/5/16; 12:12
 * @Version : 1.0
 */
public class CustomOutputFormatDriver {

    public static void main(String[] args) throws Exception{

        Path inputPath = new Path("/Users/apple/Downloads/hadoop/mrinput/outputformat/data.txt");
        Path outputPath = new Path("/Users/apple/Downloads/hadoop/output/outputformat/example1");

        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);

        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        Job job = Job.getInstance(conf);

        job.setMapperClass(CustomOutputFormatMapper.class);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setOutputFormatClass(MyCustomOutputFormat.class);

        job.setNumReduceTasks(0);

        job.waitForCompletion(true);


    }
}
