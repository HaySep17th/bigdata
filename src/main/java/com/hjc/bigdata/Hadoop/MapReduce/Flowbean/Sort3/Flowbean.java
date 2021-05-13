package com.hjc.bigdata.Hadoop.MapReduce.Flowbean.Sort3;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Description : TODO
 * @author : hjc
 * @date : 2021/5/13; 20:22
 * @Version : 1.0
 */
public class Flowbean implements WritableComparable <Flowbean> {

    private long upFlow;
    private long downFlow;
    private Long sumFlow;

    public Flowbean() {
    }

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

    @Override
    public int compareTo(Flowbean o) {
        return this.sumFlow.compareTo(o.getSumFlow());
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
        return upFlow +
                "\t" + downFlow +
                "\t" + sumFlow;
    }
}
