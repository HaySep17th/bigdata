package com.hjc.bigdata.Hadoop.MapReduce.Flowbean.CustomRawComparator;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Description : TODO
 * @author : hjc
 * @date : 2021/5/13; 20:51
 * @Version : 1.0
 */
/*
如果自定义的Key类型只实现了Writable借口，没有实现WritableComparator借口，需要提供自定义的比较器
    自定义比较器的方法之一：实现RawComparator接口
 */
public class Flowbean implements Writable {

    private long upFlow;
    private long downFlow;
    private Long sumFlow;

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public Long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(Long sumFlow) {
        this.sumFlow = sumFlow;
    }

    public Flowbean(){

    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        upFlow = in.readLong();
        downFlow = in.readLong();
        sumFlow = in.readLong();
    }

    @Override
    public String toString() {
        return  upFlow + "\t" + downFlow + "\t" + sumFlow;
    }
}
