package com.tq.hdfs.mapper;

import com.tq.hdfs.serials.OrderJoinWritable;
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
public class OrderMapper extends Mapper<LongWritable, Text, OrderWritable, NullWritable> {


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



        if(currentFile.contains("order")){//1001	01	1
            Integer orderId=Integer.valueOf(parts[0]);
            String pId=parts[1];
            Integer count=Integer.valueOf(parts[2]);
            OrderWritable orderJoinWritable=new OrderWritable(orderId,pId,count,"","order");
            context.write(orderJoinWritable,NullWritable.get());
        }else if (currentFile.contains("p")){//01	小米
            String pId=parts[0];
            String pName=parts[1];
            OrderWritable orderJoinWritable=new OrderWritable(0,pId,0,pName,"product");
            context.write(orderJoinWritable,NullWritable.get());
        }else {
            System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!");
        }





    }
}
