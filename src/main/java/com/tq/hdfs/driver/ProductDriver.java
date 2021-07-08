package com.tq.hdfs.driver;

import com.tq.hdfs.group.OrderGroupComparator;
import com.tq.hdfs.mapper.PhoneMapper;
import com.tq.hdfs.mapper.ProductMapper;
import com.tq.hdfs.patitioner.NumberPartitioner;
import com.tq.hdfs.reducer.PhoneReducer;
import com.tq.hdfs.reducer.ProductReducer;
import com.tq.hdfs.serials.PhoneMapWritable;
import com.tq.hdfs.serials.PhoneReduceWritable;
import com.tq.hdfs.serials.ProductMapWritable;
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
 * @date 2021-06-21 17:27
 * @Copyright © 2018-2019 *******
 */
public class ProductDriver {
    private static Configuration conf;
    static {
        conf=new Configuration();
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job= Job.getInstance(conf);
        job.setJarByClass(ProductDriver.class);
        job.setMapperClass(ProductMapper.class);
        job.setReducerClass(ProductReducer.class);


        job.setMapOutputKeyClass(ProductMapWritable.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(ProductMapWritable.class);
        job.setOutputValueClass(NullWritable.class);

        //分组比较器 进入reduce 前 按照KEY分组
        job.setGroupingComparatorClass(OrderGroupComparator.class);
        FileInputFormat.setInputPaths(job,new Path("D:\\htest\\product\\input.txt"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\htest\\product\\output"));

        job.waitForCompletion(true);

    }
}
