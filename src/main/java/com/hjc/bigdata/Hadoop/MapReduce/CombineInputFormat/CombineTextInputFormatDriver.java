package com.hjc.bigdata.Hadoop.MapReduce.CombineInputFormat;

import com.hjc.bigdata.Hadoop.MapReduce.WordCount.WordCountMapper;
import com.hjc.bigdata.Hadoop.MapReduce.WordCount.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * @Description : TODO
 * @author : hjc
 * @date : 2021/5/10; 21:50
 * @Version : 1.0
 */
public class CombineTextInputFormatDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {

        Path inputPath = new Path("/Users/apple/Downloads/hadoop/mrinput/combine");
        Path outputPath = new Path("/Users/apple/Downloads/hadoop/output/combine");

        Configuration conf = new Configuration();
        // 分隔符只是一个byte类型的数据，即便传入的是个字符串，只会取字符串的第一个字符
        conf.set("mapreduce.input.fileinputformat.split.maxsize", "2048");

        FileSystem fs = FileSystem.get(conf);

        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        //创建job
        Job job = Job.getInstance(conf);
        job.setJobName("CombineTextInputFormat");
        //设置job
        job.setMapperClass(CombineTextInputFormatMapper.class);
        job.setReducerClass(CombineTextInputFormatReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置输入格式
        job.setInputFormatClass(CombineTextInputFormat.class);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        //运行job

        job.waitForCompletion(true);
    }

}
