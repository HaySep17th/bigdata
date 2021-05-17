package com.hjc.bigdata.Hadoop.Example.thrid;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @Description : TODO
 * @author : hjc
 * @date : 2021/5/17; 21:40
 * @Version : 1.0
 */
public class FriendDriver {

    public static void main(String[] args) throws Exception {
        //定义路径
        Path inputPath = new Path("/Users/apple/Downloads/hadoop/mrinput/friend");
        Path outputPath = new Path("/Users/apple/Downloads/hadoop/output/friend");
        Path finalOutputPath = new Path("/Users/apple/Downloads/hadoop/output/friend/finalout");

        //作为整个Job的配置
        Configuration conf1 = new Configuration();
        conf1.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ":");
        Configuration conf2 = new Configuration();

        FileSystem fs = FileSystem.get(conf1);

        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        if (fs.exists(finalOutputPath)) {
            fs.delete(finalOutputPath, true);
        }

        //创建Job
        Job job1 = Job.getInstance(conf1);
        Job job2 = Job.getInstance(conf2);

        job1.setJobName("friend1");
        job2.setJobName("friend2");

        //设置Job1
        job1.setMapperClass(FriendMapper1.class);
        job1.setReducerClass(FriendReducer1.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        //设置Job1输入目录和输出目录
        FileInputFormat.setInputPaths(job1, inputPath);
        FileOutputFormat.setOutputPath(job1, outputPath);

        job1.setInputFormatClass(KeyValueTextInputFormat.class);

        //设置job2
        job2.setMapperClass(FriendMapper2.class);
        job2.setReducerClass(FriendReducer2.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        //设置job2的输出目录和输入目录
        FileInputFormat.setInputPaths(job2, outputPath);
        FileOutputFormat.setOutputPath(job2, finalOutputPath);

        //设置Job2的输入格式
        job2.setInputFormatClass(KeyValueTextInputFormat.class);

        //创建JobControl
        JobControl jobControl = new JobControl("friends");

        //创建运行的job
        ControlledJob cJob1 = new ControlledJob(job1.getConfiguration());
        ControlledJob cJob2 = new ControlledJob(job2.getConfiguration());

        //指定依赖关系
        cJob2.addDependingJob(cJob1);

        //向JobControl设置要运行哪些job
        jobControl.addJob(cJob1);
        jobControl.addJob(cJob2);

        //运行JobControll
        Thread thread = new Thread(jobControl);

        thread.setDaemon(true);
        thread.start();

        while (true) {

            if (jobControl.allFinished()) {

                System.out.println(jobControl.getSuccessfulJobList());

                return ;
            }

        }
    }
}
