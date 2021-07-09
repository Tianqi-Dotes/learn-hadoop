package com.tq.hdfs.driver;


import com.tq.hdfs.mapper.OrderJoinMapper;
import com.tq.hdfs.mapper.OrderMapJoinMapper;
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
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author tq
 * @version V1.0
 * @Package com.tq.hdfs.driver
 * @date 2021-06-18 11:44
 * @Copyright © 2018-2019 *******
 */
public class OrderMapJoinDriver {

    private static Configuration conf;
    static {
        conf=new Configuration();
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Job job= Job.getInstance(conf);
        job.addCacheFile(new URI("file:///D:/learnbigdata/src/main/resources/htest/order-mapjoin/product/p.txt"));

        job.setJarByClass(OrderMapJoinDriver.class);
        job.setMapperClass(OrderMapJoinMapper.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(0);//不适用reducer
        FileInputFormat.setInputPaths(job,new Path("D:\\learnbigdata\\src\\main\\resources\\htest\\order-mapjoin\\order"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\learnbigdata\\src\\main\\resources\\htest\\order-mapjoin\\out2"));

        job.waitForCompletion(true);


    }

}
