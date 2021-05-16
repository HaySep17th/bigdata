package com.hjc.bigdata.Hadoop.MapReduce.MapJoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * @Description : TODO
 * @author : hjc
 * @date : 2021/5/16; 14:25
 * @Version : 1.0
 */

/*
 * 1. 在Hadoop中，hadoop为MR提供了分布式缓存
 * 			①用来缓存一些Job运行期间的需要的文件(普通文件，jar，归档文件(har))
 * 			②通过在Job的Configuration中，使用uri代替要缓存的文件
 * 			③分布式缓存会假设当前的文件已经上传到了HDFS，并且在集群的任意一台机器都可以访问到这个URI所代表的文件
 * 			④分布式缓存会在每个节点的task运行之前，提前将文件发送到节点
 * 			⑤分布式缓存的高效是由于每个Job只会复制一次文件，且可以自动在从节点对归档文件解归档
 */
public class MapJoinMapper extends Mapper<LongWritable, Text, JoinBean, NullWritable> {

    private JoinBean out_key = new JoinBean();
    private Map<String, String> pdDatas = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException {
        URI[] files = context.getCacheFiles();

        for (URI uri : files) {
            //BufferedReader reader = new BufferedReader(new FileReader(new File(uri)));
            BufferedReader reader = new BufferedReader(new FileReader(new File(uri)));
            String line = " ";

            while (StringUtils.isNotBlank(line = reader.readLine())) {
                String[] words = line.split("\t");

                pdDatas.put(words[0], words[1]);
            }

            reader.close();
        }

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] values = value.toString().split("\t");

        out_key.setOrderId(values[0]);
        out_key.setPname(pdDatas.get(values[1]));
        out_key.setAmount(values[2]);

        context.write(out_key, NullWritable.get());
    }
}

