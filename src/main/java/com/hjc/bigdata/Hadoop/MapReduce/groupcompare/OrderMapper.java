package com.hjc.bigdata.Hadoop.MapReduce.groupcompare;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Description : TODO
 * @author : hjc
 * @date : 2021/5/15; 21:53
 * @Version : 1.0
 */
public class OrderMapper extends Mapper <LongWritable, Text, OrderBean, NullWritable> {

    private OrderBean out_key = new OrderBean();
    private NullWritable out_value = NullWritable.get();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] values = value.toString().split("\t");

        out_key.setOrderId(values[0]);
        out_key.setpId(values[1]);
        out_key.setAcount(Double.parseDouble(values[2]));

        context.write(out_key, out_value);

    }
}
