package com.hjc.bigdata.Hadoop.MapReduce.NLineTextInputFormat;

import com.hjc.bigdata.Hadoop.MapReduce.WordCount.WordCountMapper;
import com.hjc.bigdata.Hadoop.MapReduce.WordCount.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * @Description : TODO
 * @author : hjc
 * @date : 2021/5/10; 21:35
 * @Version : 1.0
 */
public class NLineTextInputFormatDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {

        Path inputPath = new Path("/Users/apple/Downloads/hadoop/mrinput/nline");
        Path outputPath = new Path("/Users/apple/Downloads/hadoop/output/nline/");

        Configuration conf = new Configuration();
        conf.set("mapreduce.input.lineinputformat.linespermap", "2");

        FileSystem fs = FileSystem.get(conf);

        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        //创建job
        Job job = Job.getInstance(conf);
        job.setJobName("NlineInputFormat");
        //设置job
        job.setMapperClass(NLineTextInputFormatMapper.class);
        job.setReducerClass(NLineTextInputFormatReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(NLineInputFormat.class);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        //运行job

        job.waitForCompletion(true);
    }


}
