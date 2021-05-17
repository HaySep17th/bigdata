package com.hjc.bigdata.Hadoop.Example.first;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
 * @date : 2021/5/17; 20:36
 * @Version : 1.0
 */

/*
 * 1. Driver 提交两个Job
 * 			Job2 必须 依赖于 Job1，必须在Job1已经运行完成之后，生成结果后，才能运行！
 *
 * 2. JobControl: 定义一组MR jobs，还可以指定其依赖关系
 * 				可以通过addJob(ControlledJob aJob)向一个JobControl中添加Job对象！
 *
 * 3. ControlledJob: 可以指定依赖关系的Job对象
 * 			addDependingJob(ControlledJob dependingJob): 为当前Job添加依赖的Job
 * 			 public ControlledJob(Configuration conf) : 基于配置构建一个ControlledJob
 *
 */
public class firstDrive {

    public static void main(String[] args) throws Exception {

        //定义路径
        Path inputPath = new Path("/Users/apple/Downloads/hadoop/mrinput/index");
        Path outputPath = new Path("/Users/apple/Downloads/hadoop/output/index");
        Path finalOutputPath = new Path("/Users/apple/Downloads/hadoop/output/index/final");

        //作为整个Job的配置
        Configuration conf1 = new Configuration();
        Configuration conf2 = new Configuration();
        //设置分隔符
        conf2.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "-");

        //保证输出目录不存在
        FileSystem fs = FileSystem.get(conf1);
        if (fs.exists(finalOutputPath)) {
            fs.delete(outputPath, true);
        }

        if (fs.exists(finalOutputPath)) {
            fs.delete(finalOutputPath, true);
        }

        //创建Job
        Job job1 = Job.getInstance(conf1);
        Job job2 = Job.getInstance(conf2);

        //设置Job名称
        job1.setJobName("第一个job");
        job2.setJobName("第二个job");

        //设置Job1
        job1.setMapperClass(firstMapper1.class);
        job1.setReducerClass(firstReducer1.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        //设置Job1的输出目录和输入目录
        FileInputFormat.setInputPaths(job1, inputPath);
        FileOutputFormat.setOutputPath(job1, outputPath);

        //设置job2
        job2.setMapperClass(firstMapper2.class);
        job2.setReducerClass(firstReducer2.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        //设置Job2的输出和输入目录
        FileInputFormat.setInputPaths(job2, outputPath);
        FileOutputFormat.setOutputPath(job2, finalOutputPath);

        //设置Job2的输入格式
        job2.setInputFormatClass(KeyValueTextInputFormat.class);

        //创建JobControl
        JobControl jobControl = new JobControl("index");

        //创建需要运行的job任务
        ControlledJob cJob1 = new ControlledJob(job1.getConfiguration());
        ControlledJob cJob2 = new ControlledJob(job2.getConfiguration());

        //指定任务之间的依赖关系，这个实例中job2依赖job1
        cJob2.addDependingJob(cJob1);

        //向JobControl设置需要运行的Job
        jobControl.addJob(cJob1);
        jobControl.addJob(cJob2);

        //运行JobControl
        Thread jobControlThread = new Thread(jobControl);
        //设置此线程为守护线程
        jobControlThread.setDaemon(true);

        jobControlThread.start();

        //获取JobControl线程的运行状态
        while (true) {

            //判断整个jobControl是否全部运行结束
            if (jobControl.allFinished()) {

                System.out.println(jobControl.getSuccessfulJobList());

                return ;
            }
        }


    }
}
