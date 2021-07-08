package com.tq.hdfs.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.io.OutputStream;

/**
 * @author tq
 * @version V1.0
 * @Package com.tq.hdfs.outputformat
 * @date 2021-06-24 10:17
 * @Copyright © 2018-2019 *******
 * reducer输入
 */
public class LogOutput extends FileOutputFormat<Text, NullWritable> {


    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        LogRecordWriter writer=new LogRecordWriter(taskAttemptContext.getConfiguration());
        return writer;
    }

    private class LogRecordWriter extends RecordWriter<Text,NullWritable>{

        private String tqPath="D:\\htest\\log\\output\\tq";
        private String otherPath="D:\\htest\\log\\output\\other";

        private FSDataOutputStream tq;
        private FSDataOutputStream other;
        private FileSystem fileSystem;

        public LogRecordWriter(Configuration c) throws IOException {
            fileSystem=FileSystem.get(c);
            tq= fileSystem.create(new Path(tqPath));
            other=fileSystem.create(new Path(otherPath));
        }

        @Override
        public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {

            String line=text.toString();
            if(line.contains("tq")){
                tq.writeBytes(text.toString()+"\n");
            }else {
                other.writeBytes(text.toString()+"\n");
            }
        }
        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            IOUtils.closeStream(tq);
            IOUtils.closeStream(other);

        }
    }
}
