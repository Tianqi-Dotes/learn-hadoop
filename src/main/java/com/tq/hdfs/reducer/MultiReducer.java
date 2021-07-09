package com.tq.hdfs.reducer;

import com.tq.hdfs.serials.MultiMapWritable;
import com.tq.hdfs.serials.OrderWritable;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author tq
 * @version V1.0
 * @Package com.tq.hdfs.reducer
 * @date 2021-07-05 10:35
 * @Copyright © 2018-2019 *******
 */
public class MultiReducer extends Reducer<Text, Text, Text, Text> {

    @Override //key word  a,b,c
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        String word=key.toString();
        Map<String,Integer> counter=new HashMap<>();
        for (Text value : values) {
            if(!counter.containsKey(value.toString())){
                counter.put(value.toString(),1);
            }else {
                Integer v=counter.get(value.toString());
                v=v+1;
                counter.put(value.toString(),v);
            }
        }
        StringBuffer sb=new StringBuffer();
        Set<String> strings = counter.keySet();
        strings.forEach(k->{
            sb.append(k);
            sb.append("---->");
            sb.append(counter.get(k)+"    ");
        });
        context.write(key,new Text(sb.toString()));

    }
}
