package com.hjc.bigdata.Hadoop.MapReduce.Partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Description : TODO
 * @author : hjc
 * @date : 2021/5/11; 20:21
 * @Version : 1.0
 */
public class MyPartition extends Partitioner <Text, Flowbean> {
    @Override
    public int getPartition(Text text, Flowbean flowbean, int numPartitions) {

        String suffix = text.toString().substring(0, 3);

        //int partNum = 0;
        switch (suffix) {
            case "136" :
                numPartitions = 1;
                break;
            case "137" :
                numPartitions = 2;
                break;
            case "138" :
                numPartitions = 3;
                break;
            case "139" :
                numPartitions = 4;
                break;
            default:
                numPartitions = 0;
                break;
        }
        return numPartitions;

    }
}
