package com.tq.hdfs.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
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
public class LogReducer extends Reducer<Text, NullWritable,Text,NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        for (NullWritable value : values) {
            context.write(key,NullWritable.get());
        }
    }
}
