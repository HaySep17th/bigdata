package com.hjc.bigdata.Hadoop.MapReduce.CostomInputFormat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @Description : TODO
 * @author : hjc
 * @date : 2021/5/10; 21:48
 * @Version : 1.0
 */
public class MyInputFormatReducer extends Reducer<Text, BytesWritable, Text, BytesWritable> {
}
