package com.tq.hdfs.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author tq
 * @version V1.0
 * @Package com.tq.hdfs.reducer
 * @date 2021-06-18 11:43
 * @Copyright © 2018-2019 *******
 */
public class WordcountReducer extends Reducer<Text, IntWritable,Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        Integer sum=new Integer(0);
        Iterator<IntWritable> vs=values.iterator();
        while (vs.hasNext()){
            sum+=vs.next().get();
        }
        context.write(key,new IntWritable(sum));
    }
}
