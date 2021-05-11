package com.hjc.bigdata.Hadoop.MapReduce.FlowbeanSort2;

import org.apache.hadoop.io.WritableComparator;

/**
 * @Description : TODO
 * @author : hjc
 * @date : 2021/5/11; 21:15
 * @Version : 1.0
 */
public class MyComparator extends WritableComparator {

    @Override
    public int compare(byte[] b1, int s1, int l1,
                       byte[] b2, int s2, int l2) {
        long thisValue = readLong(b1, s1);
        long thatValue = readLong(b2, s2);
        return (thisValue<thatValue ? 1 : (thisValue==thatValue ? 0 : -1));
    }
}
