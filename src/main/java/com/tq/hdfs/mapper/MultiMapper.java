package com.tq.hdfs.mapper;

import com.tq.hdfs.serials.MultiMapWritable;
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
public class MultiMapper extends Mapper<LongWritable, Text, Text, Text> {


    private String currentFile;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //获取当前的切片对象
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        currentFile= inputSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line=value.toString();
        String[] parts=line.split("\t");
        String word1=parts[0];
        String word2=parts[1];
        Text a=new Text("a");
        Text b=new Text("b");
        Text c=new Text("c");


        if(currentFile.contains("a")){//xx  xx

            context.write(new Text(word1),a);
            context.write(new Text(word2),a);

        }else if (currentFile.contains("b")){//01	小米

            context.write(new Text(word1),b);
            context.write(new Text(word2),b);

        }else {//c
            context.write(new Text(word1),c);
            context.write(new Text(word2),c);
        }
    }
}
