package com.hjc.bigdata.Hadoop.MapReduce.groupcompare;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;

import java.io.IOException;

/**
 * @Description : TODO
 * @author : hjc
 * @date : 2021/5/15; 21:53
 * @Version : 1.0
 */
public class MyComparator implements RawComparator<OrderBean> {

    private OrderBean key1 = new OrderBean();
    private OrderBean key2 = new OrderBean();
    private DataInputBuffer buffer = new DataInputBuffer();

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        try {
            buffer.reset(b1, s1, l1);
            key1.readFields(buffer);

            buffer.reset(b2, s2, l2);
            key2.readFields(buffer);

            buffer.reset(null, 0, 0);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return compare(key1, key2);
    }

    @Override
    public int compare(OrderBean o1, OrderBean o2) {
        return o1.getOrderId().compareTo(o2.getOrderId());
    }
}
