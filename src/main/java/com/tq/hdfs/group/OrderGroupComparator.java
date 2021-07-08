package com.tq.hdfs.group;

import com.tq.hdfs.serials.ProductMapWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * reduce 分组 key 比较规则定义
 * @author tq
 * @version V1.0
 * @Package com.tq.hdfs.group
 * @date 2021-06-23 16:01
 * @Copyright © 2018-2019 *******
 */
public class OrderGroupComparator extends WritableComparator {

    public OrderGroupComparator(){
        super(ProductMapWritable.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        ProductMapWritable aa=(ProductMapWritable)a;
        ProductMapWritable bb=(ProductMapWritable)b;

        return aa.getOrderId().compareTo(bb.getOrderId());

    }
}
