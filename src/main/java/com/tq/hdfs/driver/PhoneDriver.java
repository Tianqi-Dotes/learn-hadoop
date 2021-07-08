package com.tq.hdfs.driver;

import com.tq.hdfs.mapper.PhoneMapper;
import com.tq.hdfs.mapper.WordcountMapper;
import com.tq.hdfs.patitioner.NumberPartitioner;
import com.tq.hdfs.reducer.PhoneReducer;
import com.tq.hdfs.reducer.WordcountReducer;
import com.tq.hdfs.serials.PhoneMapWritable;
import com.tq.hdfs.serials.PhoneReduceWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
public class PhoneDriver {
    private static Configuration conf;
    static {
        conf=new Configuration();
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job= Job.getInstance(conf);
        job.setJarByClass(PhoneDriver.class);
        job.setMapperClass(PhoneMapper.class);
        job.setReducerClass(PhoneReducer.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PhoneMapWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PhoneReduceWritable.class);

        //设置分区器
        job.setPartitionerClass(NumberPartitioner.class);
        //按照分区业务逻辑设置个数
        job.setNumReduceTasks(3);
        FileInputFormat.setInputPaths(job,new Path("D:\\htest\\phoneInput\\input.txt"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\htest\\phoneInput\\phoneOut2"));

        job.waitForCompletion(true);

    }
}
