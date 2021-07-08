package com.tq.hdfs.mapper;

import com.tq.hdfs.serials.ProductMapWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author tq
 * @version V1.0
 * @Package com.tq.hdfs.mapper
 * @date 2021-06-21 17:27
 * @Copyright © 2018-2019 *******
 */
public class ProductMapper extends Mapper<LongWritable, Text, ProductMapWritable, NullWritable> {

    @Override
    protected void map(LongWritable number, Text value, Context context) throws IOException, InterruptedException {

        String v=value.toString();
        //10000001	prod_01	222.8
        String[] splits=v.split("\t");
        String orderId=splits[0];
        String prodId=splits[1];
        Double price=Double.valueOf(splits[2]);

        ProductMapWritable productMapWritable=new ProductMapWritable(orderId,prodId,price);
        context.write(productMapWritable,NullWritable.get());
    }
}
