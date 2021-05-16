package com.hjc.bigdata.Hadoop.MapReduce.ReduceJoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Description : TODO
 * @author : hjc
 * @date : 2021/5/16; 15:17
 * @Version : 1.0
 */
public class ReducerJoinReducer extends Reducer <NullWritable, JoinBean, NullWritable, JoinBean> {

    private List<JoinBean> orderDatas = new ArrayList<>();
    private Map<String, String> pdDatas = new HashMap<>();

    @Override
    protected void reduce(NullWritable key, Iterable<JoinBean> values, Context context) throws IOException, InterruptedException {

        for (JoinBean value : values) {

            if (value.getSource().equals("order.txt")) {
                JoinBean joinBean = new JoinBean();

                try {
                    BeanUtils.copyProperties(joinBean, value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }

                orderDatas.add(joinBean);
            } else {
                pdDatas.put(value.getpId(), value.getPname());
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        //只输出来自orderDatas的数据
        for (JoinBean joinBean : orderDatas) {

            // 从Map中根据pid取出pname，设置到bean的pname属性中
            joinBean.setPname(pdDatas.get(joinBean.getpId()));

            context.write(NullWritable.get(), joinBean);

        }
    }
}
