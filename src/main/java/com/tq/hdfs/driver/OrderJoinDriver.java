package com.tq.hdfs.driver;


import com.tq.hdfs.mapper.OrderJoinMapper;
import com.tq.hdfs.reducer.OrderJoinReducer;
import com.tq.hdfs.serials.OrderJoinWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author tq
 * @version V1.0
 * @Package com.tq.hdfs.driver
 * @date 2021-06-18 11:44
 * @Copyright © 2018-2019 *******
 */
public class OrderJoinDriver {

    private static Configuration conf;
    static {
        conf=new Configuration();
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job= Job.getInstance(conf);
        job.setJarByClass(OrderJoinDriver.class);
        job.setMapperClass(OrderJoinMapper.class);
        job.setReducerClass(OrderJoinReducer.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(OrderJoinWritable.class);

        job.setOutputKeyClass(OrderJoinWritable.class);
        job.setOutputValueClass(NullWritable.class);


        FileInputFormat.setInputPaths(job,new Path("D:\\htest\\order-join"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\htest\\order-join\\output2"));

        job.waitForCompletion(true);


    }

}
