package com.tq.hdfs.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author tq
 * @version V1.0
 * @Package com.tq.hdfs.mapper
 * @date 2021-06-18 11:43
 * @Copyright © 2018-2019 *******
 */
public class LogMapper extends Mapper<LongWritable,Text, Text, NullWritable> {

    private Text resutltKey=new Text();
    private static final IntWritable oneValue=new IntWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        context.write(value,NullWritable.get());
    }
}
