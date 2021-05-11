package com.hjc.bigdata.Hadoop.MapReduce.Partition;

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
 * @date : 2021/5/10; 21:43
 * @Version : 1.0
 */
public class FlowbeanDriver {
    public static void main(String[] args) throws Exception {
        Path inputPath = new Path("/Users/apple/Downloads/hadoop/mrinput/flowbean/data.txt");
        Path outputPath = new Path("/Users/apple/Downloads/hadoop/output/partition");

        //作为整个Job的配置
        Configuration conf = new Configuration();

        //保证输出目录不存在
        FileSystem fs = FileSystem.get(conf);

        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        //创建Job
        Job job = Job.getInstance(conf);
        job.setJobName("flowbean");

        //设置Job
        //设置Job运行的Mapper，Reducer类型，Mapper,Reducer输出的key-value类型
        job.setMapperClass(FlowbeanMapper.class);
        job.setReducerClass(FlowbeanReducer.class);

        // Job需要根据Mapper和Reducer输出的Key-value类型准备序列化器，通过序列化器对输出的key-value进行序列化和反序列化
        // 如果Mapper和Reducer输出的Key-value类型一致，直接设置Job最终的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Flowbean.class);

        //设置输入目录和输出目录
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        //设置reducer数量
        job.setNumReduceTasks(5);
        //设置使用自定义的分区器
        job.setPartitionerClass(MyPartition.class);
        //运行Job
        job.waitForCompletion(true);

    }
}
