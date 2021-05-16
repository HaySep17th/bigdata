package com.hjc.bigdata.Hadoop.MapReduce.MapJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

/**
 * @Description : TODO
 * @author : hjc
 * @date : 2021/5/16; 14:25
 * @Version : 1.0
 */
public class MapJoinDriver {

    public static void main(String[] args) throws Exception {
        Path inputPath = new Path("/Users/apple/Downloads/hadoop/mrinput/mapjoin/order.txt");
        Path outputPath = new Path("/Users/apple/Downloads/hadoop/output/mapjoin/");

        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);

        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        Job job = Job.getInstance(conf);
        job.setJobName("Mapper实现join");

        job.setMapperClass(MapJoinMapper.class);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        //设置分布式缓存
        job.addCacheFile(new URI("file:///Users/apple/Downloads/hadoop/mrinput/reducejoin/pd.txt"));

        job.setNumReduceTasks(0);

        job.waitForCompletion(true);
    }
}
