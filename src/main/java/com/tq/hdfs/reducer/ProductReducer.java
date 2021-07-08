package com.tq.hdfs.reducer;

import com.tq.hdfs.serials.OrderJoinWritable;
import com.tq.hdfs.serials.PhoneMapWritable;
import com.tq.hdfs.serials.PhoneReduceWritable;
import com.tq.hdfs.serials.ProductMapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author tq
 * @version V1.0
 * @Package com.tq.hdfs.reducer
 * @date 2021-06-21 17:27
 * @Copyright © 2018-2019 *******
 */
public class ProductReducer extends Reducer<Text, OrderJoinWritable,OrderJoinWritable, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<OrderJoinWritable> values, Context context) throws IOException, InterruptedException {

        Map<String,String> products=new HashMap<String, String>();
        for (OrderJoinWritable value : values) {
            if(value.getFlag().equals("product")){
                products.put(value.getPId(),value.getPName());
            }
        }
        for (OrderJoinWritable value : values) {
            if(value.getFlag().equals("order")){
                value.setPName(products.get(value.getPId()));
            }
            context.write(value,NullWritable.get());
        }


    }
}
