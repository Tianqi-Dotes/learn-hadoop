package com.tq.hdfs.reducer;

import com.tq.hdfs.serials.OrderJoinWritable;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author tq
 * @version V1.0
 * @Package com.tq.hdfs.reducer
 * @date 2021-07-05 10:35
 * @Copyright © 2018-2019 *******
 */
public class OrderJoinReducer extends Reducer<Text, OrderJoinWritable,OrderJoinWritable, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<OrderJoinWritable> values, Context context) throws IOException, InterruptedException {

        String pName = null;
        List<OrderJoinWritable> orders=new ArrayList<OrderJoinWritable>();
        for (OrderJoinWritable value : values) {
            if(value.getFlag().equals("product")){
                pName=value.getPName();
            }else {
                OrderJoinWritable o=new OrderJoinWritable();
                try {
                    BeanUtils.copyProperties(o,value);
                    orders.add(o);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }
        for (OrderJoinWritable order : orders) {
            order.setPName(pName);
            context.write(order,NullWritable.get());
        }
        /*for (OrderJoinWritable value : values) {
            if(value.getFlag().equals("order")){
                value.setPName(pName);
            }
            context.write(value,NullWritable.get());
        }*/
    }
}
