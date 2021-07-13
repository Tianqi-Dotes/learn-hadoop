package com.tq.hdfs.reducer;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author tq
 * @version V1.0
 * @Package com.tq.hdfs.reducer
 * @date 2021-06-18 11:43
 * @Copyright © 2018-2019 *******
 */
public class FriendsReducer extends Reducer<Text, Text,Text,Text> {

    //A ---X,X,X,X,X,X
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {


    }
}
