package com.hjc.bigdata.Hadoop.MapReduce.ReduceJoin;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Description : TODO
 * @author : hjc
 * @date : 2021/5/16; 15:18
 * @Version : 1.0
 */
public class ReduceJoinPartitioner extends Partitioner<NullWritable, JoinBean>{

    @Override
    public int getPartition(NullWritable key, JoinBean value, int numPartitions) {

        return (value.getpId().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }

}
