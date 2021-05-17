package com.hjc.bigdata.Hadoop.Example.first;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @Description : TODO
 * @author : hjc
 * @date : 2021/5/17; 20:37
 * @Version : 1.0
 */

/*
 * 1.输入
 * 		hjc-a.txt\t3
 * 		hjc-b.txt\t3
 * 		使用KeyValueTextInputFormat,可以使用一个分隔符，分隔符之前的作为key，之后的作为value
 * 2.输出
 * 		hjc,a.txt\t3
 * 		hjc,b.txt\t3
 */
public class firstMapper2 extends Mapper <Text, Text, Text, Text> {
}
