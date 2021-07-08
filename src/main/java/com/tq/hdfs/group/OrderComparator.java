package com.tq.hdfs.group;

import com.tq.hdfs.serials.OrderWritable;
import com.tq.hdfs.serials.ProductMapWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author tq
 * @version V1.0
 * @Package com.tq.hdfs.group
 * @date 2021-07-06 17:11
 * @Copyright © 2018-2019 *******
 * 分组
 */
public class OrderComparator extends WritableComparator {

    public OrderComparator(){
        super(OrderWritable.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderWritable o1= (OrderWritable) a;
        OrderWritable o2= (OrderWritable) b;

        return o1.getPId().compareTo(((OrderWritable) b).getPId());

    }
}
