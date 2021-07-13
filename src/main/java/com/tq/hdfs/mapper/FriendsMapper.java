package com.tq.hdfs.mapper;

import com.tq.hdfs.serials.OrderWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author tq
 * @version V1.0
 * @Package com.tq.hdfs.mapper
 * @date 2021-07-05 10:35
 * @Copyright © 2018-2019 *******
 */
public class FriendsMapper extends Mapper<LongWritable, Text, Text, Text> {


    private String currentFile;


    @Override//A:XXX,X,X,X,X,--》X:A X:A
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line=value.toString();
        String[] parts=line.split(":");

        String host=parts[0];
        String fs=parts[1];
        String[] f=fs.split(",");
        for (String s : f) {

            context.write(new Text(s),new Text(host));
        }
    }
}
