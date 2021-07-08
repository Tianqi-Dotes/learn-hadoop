package com.tq.hdfs.mapper;

import com.tq.hdfs.serials.PhoneMapWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

import java.io.IOException;

/**
 * @author tq
 * @version V1.0
 * @Package com.tq.hdfs.mapper
 * @date 2021-06-21 17:27
 * @Copyright © 2018-2019 *******
 */
public class PhoneMapper extends Mapper<LongWritable, Text, Text, PhoneMapWritable> {

    @Override
    protected void map(LongWritable number, Text value, Context context) throws IOException, InterruptedException {

        String v=value.toString();
        //1 110 192.168.0.1 baidu.com   3767    9810    200
        String[] splits=v.split("\t");
        String num=splits[1];
        Integer up=Integer.valueOf(splits[4]);
        Integer down=Integer.valueOf(splits[5]);

        PhoneMapWritable phoneMapWritable=new PhoneMapWritable(up,down);
        context.write(new Text(num),phoneMapWritable);
    }
}
