package com.tq.hdfs.reducer;

import com.tq.hdfs.serials.PhoneMapWritable;
import com.tq.hdfs.serials.PhoneReduceWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author tq
 * @version V1.0
 * @Package com.tq.hdfs.reducer
 * @date 2021-06-21 17:27
 * @Copyright © 2018-2019 *******
 */
public class PhoneReducer extends Reducer<Text, PhoneMapWritable,Text, PhoneReduceWritable> {

    @Override
    protected void reduce(Text key, Iterable<PhoneMapWritable> values, Context context) throws IOException, InterruptedException {

        Integer uptotal=new Integer(0);
        Integer downtotal=new Integer(0);
        Integer total= 0;

        for (PhoneMapWritable value : values) {
            uptotal+=value.getUpCount();
            downtotal+=value.getDownCount();
            total+=value.getUpCount();
            total+=value.getDownCount();
        }
        PhoneReduceWritable writable=new PhoneReduceWritable(uptotal,downtotal,total);

        context.write(key,writable);
    }
}
