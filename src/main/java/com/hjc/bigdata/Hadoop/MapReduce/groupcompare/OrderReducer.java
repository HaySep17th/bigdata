package com.hjc.bigdata.Hadoop.MapReduce.groupcompare;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Description : TODO
 * @author : hjc
 * @date : 2021/5/15; 21:54
 * @Version : 1.0
 */

/*
 * OrderBean key-NullWritable nullWritable在reducer工作期间，
 * 	只会实例化一个key-value的对象！
 * 		每次调用迭代器迭代下个记录时，使用反序列化器从文件中或内存中读取下一个key-value数据的值，
 * 		封装到之前OrderBean key-NullWritable nullWritable在reducer的属性中
 */
public class OrderReducer extends Reducer <OrderBean, NullWritable,OrderBean, NullWritable> {

    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        Double maxAcount = key.getAcount();

        for (NullWritable value : values) {

            if (!(key.getAcount().equals(maxAcount))) {
                break;
            }

            //将符合条件的记录写出
            context.write(key, value);
        }

    }
}
