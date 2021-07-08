package com.tq.hdfs.mapper;

import com.tq.hdfs.serials.OrderWritable;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.logging.log4j.util.Strings;
import org.checkerframework.checker.units.qual.C;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @author tq
 * @version V1.0
 * @Package com.tq.hdfs.mapper
 * @date 2021-07-05 10:35
 * @Copyright © 2018-2019 *******
 * 小表维护内存
 * mapper读大表  取内存值 不需要reducer
 */
public class OrderMapJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private Map<String,String> cache=new HashMap<String, String>();

    @Override
    //处理大表之前 把小表维护内存中
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();
        final URI cacheFile = cacheFiles[0];
        FileSystem fs=FileSystem.get(context.getConfiguration());
        FSDataInputStream open = fs.open(new Path(cacheFile));
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(open,"utf8"));

        bufferedReader.lines().forEach(e->{
            String[] split = e.split("\t");
            cache.put(split[0],split[1]);
        });
        IOUtils.closeStream(bufferedReader);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line=value.toString();
        String[] parts=line.split("\t");


        parts[1]=cache.get(parts[1]);


        StringBuffer sb=new StringBuffer();
        for (int i=0;i<parts.length;i++) {
            if(i!=2){
                sb.append(parts[i]);
                sb.append("\t");
            }else {
                sb.append(parts[i]);
            }
        }
        context.write(new Text(sb.toString()),NullWritable.get());
    }
}
