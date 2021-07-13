package com.tq.hdfs.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * @author tq
 * @version V1.0
 * @Package com.tq.hdfs.compress
 * @date 2021-07-13 16:39
 * @Copyright © 2018-2019 *******
 */
public class TestCompress {

    public static void main2(String[] args) throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException {
        String file="D:\\learnbigdata\\src\\main\\resources\\htest\\compress\\bailuyuan.txt";
        FileInputStream fileInputStream = new FileInputStream(new File(file));

        String codecClassName="org.apache.hadoop.io.compress.DefaultCodec";
        Class c=Class.forName(codecClassName);
        /*DefaultCodec defaultCode= (DefaultCodec) c.newInstance();
        CompressionCodec cc=defaultCode;*/
        Configuration conf=new Configuration();
        CompressionCodec cc = (CompressionCodec) ReflectionUtils.newInstance(c, conf);

        FileOutputStream fileOutputStream = new FileOutputStream(new File(file+ cc.getDefaultExtension()) );
        CompressionOutputStream stream=cc.createOutputStream(fileOutputStream);
        IOUtils.copyBytes(fileInputStream,stream,new Configuration());

        IOUtils.closeStream(fileInputStream);
        IOUtils.closeStream(fileOutputStream);
    }

    public static void main(String[] args) throws IOException {
        String file="D:\\learnbigdata\\src\\main\\resources\\htest\\compress\\bailuyuan.txt.deflate";
        Configuration conf=new Configuration();
        CompressionCodec codec = new CompressionCodecFactory(conf).getCodec(new Path(file));
        FileInputStream fileInputStream = new FileInputStream(new File(file));
        CompressionInputStream inputStream = codec.createInputStream(fileInputStream);

        FileOutputStream fileOutputStream = new FileOutputStream(new File("D:\\learnbigdata\\src\\main\\resources\\htest\\compress\\bailuyuan2.txt"));

        IOUtils.copyBytes(inputStream,fileOutputStream,conf);

        IOUtils.closeStream(inputStream);
        IOUtils.closeStream(fileOutputStream);

    }
}
