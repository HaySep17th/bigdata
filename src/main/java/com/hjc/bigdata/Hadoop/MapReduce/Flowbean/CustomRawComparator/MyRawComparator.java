package com.hjc.bigdata.Hadoop.MapReduce.Flowbean.CustomRawComparator;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;

import java.io.IOException;

/**
 * @Description : TODO
 * @author : hjc
 * @date : 2021/5/13; 20:50
 * @Version : 1.0
 */
public class MyRawComparator implements RawComparator <Flowbean> {

    private Flowbean key1 = new Flowbean();
    private Flowbean key2 = new Flowbean();
    private DataInputBuffer buffer = new DataInputBuffer();

    //负责从缓冲区中解析出要比较的两个Key对象，调用compare（）对两个对象进行比较
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        try {
            buffer.reset(b1, s1, l1);                   // parse key1
            key1.readFields(buffer);

            buffer.reset(b2, s2, l2);                   // parse key2
            key2.readFields(buffer);

            buffer.reset(null, 0, 0);                   // clean up reference
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return compare(key1, key2);
    }

    @Override
    public int compare(Flowbean o1, Flowbean o2) {
        return -o1.getSumFlow().compareTo(o2.getSumFlow());
    }
}
