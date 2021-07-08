package com.tq.hdfs.driver;


import com.tq.hdfs.group.OrderComparator;
import com.tq.hdfs.mapper.OrderJoinMapper;
import com.tq.hdfs.mapper.OrderMapper;
import com.tq.hdfs.reducer.OrderJoinReducer;
import com.tq.hdfs.reducer.OrderReducer;
import com.tq.hdfs.serials.OrderJoinWritable;
import com.tq.hdfs.serials.OrderWritable;
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
public class OrderDriver {

    private static Configuration conf;
    static {
        conf=new Configuration();
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job= Job.getInstance(conf);
        job.setJarByClass(OrderDriver.class);
        job.setMapperClass(OrderMapper.class);
        job.setReducerClass(OrderReducer.class);


        job.setMapOutputKeyClass(OrderWritable.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(OrderWritable.class);
        job.setOutputValueClass(NullWritable.class);

        job.setGroupingComparatorClass(OrderComparator.class);

        FileInputFormat.setInputPaths(job,new Path("D:\\htest\\order-join"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\htest\\order-join\\output3"));

        job.waitForCompletion(true);
    }

}
