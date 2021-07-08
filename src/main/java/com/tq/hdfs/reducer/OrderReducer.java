package com.tq.hdfs.reducer;

import com.tq.hdfs.serials.OrderJoinWritable;
import com.tq.hdfs.serials.OrderWritable;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author tq
 * @version V1.0
 * @Package com.tq.hdfs.reducer
 * @date 2021-07-05 10:35
 * @Copyright © 2018-2019 *******
 */
public class OrderReducer extends Reducer<OrderWritable, NullWritable,OrderWritable, NullWritable> {

    @Override
    protected void reduce(OrderWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        String pName = null;
        List<OrderWritable> orders=new ArrayList<OrderWritable>();
        for (NullWritable value : values) {
            if(key.getFlag().equals("product")){
                pName=key.getPName();
            }else {
                OrderWritable o=new OrderWritable();
                try {
                    BeanUtils.copyProperties(o,key);
                    orders.add(o);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }
        for (OrderWritable order : orders) {
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
