package com.hjc.bigdata.Hadoop.MapReduce.ReduceJoin;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Description : TODO
 * @author : hjc
 * @date : 2021/5/16; 15:17
 * @Version : 1.0
 */
public class JoinBean implements Writable {

    private String orderId;
    private String pId;
    private String pname;
    private String amount;
    private String source;

    public JoinBean () {}

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getpId() {
        return pId;
    }

    public void setpId(String pId) {
        this.pId = pId;
    }

    public String getPname() {
        return pname;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }

    public String getAmount() {
        return amount;
    }

    public void setAmount(String amount) {
        this.amount = amount;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    @Override
    public String toString() {
        return orderId + "\t" + pname + "\t" + amount ;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(orderId);
        out.writeUTF(pId);
        out.writeUTF(pname);
        out.writeUTF(amount);
        out.writeUTF(source);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        orderId = in.readUTF();
        pId = in.readUTF();
        pname = in.readUTF();
        amount = in.readUTF();
        source = in.readUTF();

    }
}
