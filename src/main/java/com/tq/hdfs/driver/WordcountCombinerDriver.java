package com.tq.hdfs.driver;


import com.tq.hdfs.combiner.WordcountCombiner;
import com.tq.hdfs.mapper.WordcountMapper;
import com.tq.hdfs.reducer.WordcountReducer;
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
 * @date 2021-06-18 11:44
 * @Copyright © 2018-2019 *******
 */
public class WordcountCombinerDriver {

    private static Configuration conf;
    static {
        conf=new Configuration();
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job= Job.getInstance(conf);
        job.setJarByClass(WordcountCombinerDriver.class);
        job.setMapperClass(WordcountMapper.class);
        job.setReducerClass(WordcountReducer.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置combiner 初步合并 局部汇总
        job.setCombinerClass(WordcountCombiner.class);


        FileInputFormat.setInputPaths(job,new Path("D:\\htest\\input\\wordcount.txt"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\htest\\input\\out"));

        job.waitForCompletion(true);


    }

}
