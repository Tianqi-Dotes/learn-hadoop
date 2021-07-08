package com.tq.hdfs.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author tq
 * @version V1.0
 * @Package com.tq.hdfs.combiner
 * @date 2021-06-24 8:58
 * @Copyright © 2018-2019 *******
 * 输入为mapper输出  输出为reducer输入
 */
public class WordcountCombiner extends Reducer<Text, IntWritable,Text, IntWritable > {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum=0;
        for (IntWritable value : values) {
            int v=value.get();
            sum+=v;
        }
        context.write(key,new IntWritable(sum));
    }
}
