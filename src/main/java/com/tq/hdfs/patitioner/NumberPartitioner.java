package com.tq.hdfs.patitioner;

import com.tq.hdfs.serials.PhoneMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author tq
 * @version V1.0
 * @Package com.tq.hdfs.patitioner
 * @date 2021-06-23 11:23
 * @Copyright © 2018-2019 *******
 */
public class NumberPartitioner extends Partitioner<Text, PhoneMapWritable> {
    @Override
    public int getPartition(Text text, PhoneMapWritable phoneMapWritable, int i) {

        String number=text.toString();
        if(number.equals("110")){
            return 0;
        }else if (number.equals("122")){
            return 1;
        }else if (number.equals("119")){
            return 2;
        }
        return 0;
    }
}
