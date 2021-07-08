package com.tq.hdfs.driver;


import com.tq.hdfs.mapper.LogMapper;
import com.tq.hdfs.mapper.WordcountMapper;
import com.tq.hdfs.outputformat.LogOutput;
import com.tq.hdfs.reducer.LogReducer;
import com.tq.hdfs.reducer.WordcountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
public class LogDriver {

    private static Configuration conf;
    static {
        conf=new Configuration();
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job= Job.getInstance(conf);
        job.setJarByClass(LogDriver.class);
        job.setMapperClass(LogMapper.class);
        job.setReducerClass(LogReducer.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //使用自定义output
        job.setOutputFormatClass(LogOutput.class);

        FileInputFormat.setInputPaths(job,new Path("D:\\htest\\log\\input.txt"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\htest\\log\\output"));

        job.waitForCompletion(true);


    }

}
