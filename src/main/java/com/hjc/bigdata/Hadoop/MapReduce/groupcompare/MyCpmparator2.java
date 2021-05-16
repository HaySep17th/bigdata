package com.hjc.bigdata.Hadoop.MapReduce.groupcompare;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @Description : TODO
 * @author : hjc
 * @date : 2021/5/15; 21:53
 * @Version : 1.0
 */
public class MyCpmparator2 extends WritableComparator {

    public MyCpmparator2 () {
        super(OrderBean.class, null, true);
    }

    public int compare (WritableComparable a, WritableComparable b) {
        OrderBean o1 = (OrderBean) a;
        OrderBean o2 = (OrderBean) b;
        return o1.getOrderId().compareTo(o2.getOrderId());
    }

}
