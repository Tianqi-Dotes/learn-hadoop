package com.tq.hdfs.driver;


import com.tq.hdfs.group.OrderComparator;
import com.tq.hdfs.mapper.MultiMapper;
import com.tq.hdfs.mapper.OrderMapper;
import com.tq.hdfs.reducer.MultiReducer;
import com.tq.hdfs.reducer.OrderReducer;
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
public class MultiDriver {

    private static Configuration conf;
    static {
        conf=new Configuration();
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job= Job.getInstance(conf);
        job.setJarByClass(MultiDriver.class);
        job.setMapperClass(MultiMapper.class);
        job.setReducerClass(MultiReducer.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        FileInputFormat.setInputPaths(job,new Path("D:\\learnbigdata\\src\\main\\resources\\htest\\multi\\in"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\learnbigdata\\src\\main\\resources\\htest\\multi\\out"));

        job.waitForCompletion(true);
    }

}
