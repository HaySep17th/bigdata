package com.hjc.bigdata.Hadoop.MapReduce.ReduceJoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @Description : TODO
 * @author : hjc
 * @date : 2021/5/16; 15:17
 * @Version : 1.0
 */

/*
 * Map阶段无法完成Join，只能封装数据，在Reduce阶段完成Join
 *
 * 1. order.txt: 1001	01	1
 * 	 pd.txt :  01 小米
 *
 * 2. Bean必须能封装所有的数据
 *
 * 3. Reduce只需要输出来自于order.txt的数据，需要在Mapper中对数据打标记，标记数据的来源
 *
 * 4. 在Mapper中需要获取当前切片的来源，根据来源执行不同的封装逻辑
 */
public class ReducerJoinMapper extends Mapper <LongWritable, Text, NullWritable, JoinBean> {

    private NullWritable out_key = NullWritable.get();
    private JoinBean out_value = new JoinBean();
    private String source;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        FileSplit split = (FileSplit) context.getInputSplit();
        source = split.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] values = value.toString().split("\t");

        out_value.setSource(source);

        if (source.equals("order.txt")) {
            out_value.setOrderId(values[0]);
            out_value.setpId(values[1]);
            out_value.setAmount(values[2]);
            out_value.setPname("nodata");
        } else {
            out_value.setpId(values[0]);
            out_value.setPname(values[1]);
            // 保证所有的属性不为null
            out_value.setOrderId("nodata");
            out_value.setAmount("nodata");
        }

        context.write(out_key, out_value);

    }
}
