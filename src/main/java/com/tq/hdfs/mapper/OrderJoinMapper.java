package com.tq.hdfs.mapper;

import com.tq.hdfs.serials.OrderJoinWritable;
import org.apache.hadoop.io.LongWritable;
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
public class OrderJoinMapper extends Mapper<LongWritable, Text,Text, OrderJoinWritable> {


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
            OrderJoinWritable orderJoinWritable=new OrderJoinWritable(orderId,pId,count,"","order");
            context.write(new Text(pId),orderJoinWritable);
        }else if (currentFile.contains("p")){//01	小米
            String pId=parts[0];
            String pName=parts[1];
            OrderJoinWritable orderJoinWritable=new OrderJoinWritable(0,pId,0,pName,"product");
            context.write(new Text(pId),orderJoinWritable);
        }else {
            System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!");
        }





    }
}
