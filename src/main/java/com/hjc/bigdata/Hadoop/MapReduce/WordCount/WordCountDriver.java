package com.hjc.bigdata.Hadoop.MapReduce.WordCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * @Description : TODO
 * @author : hjc
 * @date : 2021/5/10; 21:32
 * @Version : 1.0
 */
/*
 * 1.一旦启动这个线程，运行Job
 *
 * 2.本地模式主要用于测试程序是否正确！
 *
 * 3. 报错：
 * 	ExitCodeException exitCode=1: /bin/bash: line 0: fg: no job control
 */
public class WordCountDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {

        //Path inputPath = new Path("/Users/apple/Downloads/HarryPotter.txt");
        //Path outputPath = new Path("/Users/apple/Downloads/hadoop/output/wprdcount/");
        Path inputPath = new Path("/wc");
        Path outputPath = new Path("/output/wprdcount/");

        //作为整个Job的配置
        Configuration conf = new Configuration();
        //conf.set("fs.defaultFS", "hdfs://hp1:9000");

        //在YARN上运行
        //conf.set("mapreduce.framework.name", "yarn");

        //ResourceManager所在的机器
        //conf.set("yarn.resourcemanager.hostname", "hp1");

        /*
        保证输出目录不存在
         */
        FileSystem fs = FileSystem.get(conf);

        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        //创建job
        Job job = Job.getInstance(conf);

        //告诉nn运行时，MR中job所在的jar包在哪
        job.setJar("hdfs-1.0-SNAPSHOT.jar");

        job.setJobName("wordcount");
        /*
        设置job
         */
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        //运行job

        job.waitForCompletion(true);
    }

}
