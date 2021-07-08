package com.tq.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author tq
 * @version V1.0
 * @Package com.tq.hdfs
 * @date 2021/1/27 17:34
 * @Copyright © 2018-2019 *******
 */
public class TestHDFS {

    private URI uri=URI.create("hdfs://hadoop001:9820");
    private Configuration conf=new Configuration();
    private String user="root";
    private FileSystem fs;

    @Before
    public void init() throws IOException, InterruptedException {
        fs=FileSystem.get(uri,conf,user);
    }

    @After
    public void close() throws IOException {
        fs.close();
    }

    @Test
    public void connectHDFS() throws Exception {

        fs.mkdirs(new Path("/clientdone"));
        fs.close();
    }

    @Test
    public void copeFile() throws Exception {

        fs.copyFromLocalFile(new Path("C:\\Users\\zuoning\\Desktop\\投放报告.xls"),new Path("/clientdone"));
        fs.close();
    }

    @Test
    public void download() throws Exception {

        fs.copyToLocalFile(new Path("/clientdone/投放报告.xls"),new Path("D:\\360Downloads\\"));
        fs.close();
    }
    @Test
    public void delete() throws Exception {

        fs.delete(new Path("/clientdone/投放报告.xls"),false );
        fs.close();
    }

    @Test
    public void move() throws Exception {

        fs.rename(new Path("/clientdone/error (1).log"),new Path("/clientdone/1.log" ));
        fs.close();
    }

    @Test
    public void listFiles() throws Exception {

        RemoteIterator<LocatedFileStatus> iterator =fs.listFiles(new Path("/"),true);
        System.out.println(iterator);
        fs.close();
    }

    @Test
    public void listStatus() throws Exception {

        FileStatus[] iterator =fs.listStatus(new Path("/"));
        for(FileStatus status:iterator){
            System.out.println(status.getPath());
            System.out.println(status.isDirectory());
        }
        fs.close();
    }

    @Test
    public void streamDownload() throws Exception {

        FileOutputStream outputStream=new FileOutputStream(new File("D:\\360Downloads\\log.log"));
        InputStream ip  =fs.open(new Path("/tmp/logs/root/logs-tfile/application_1611715175507_0054/hadoop001_41466"));

        IOUtils.copyBytes(ip,outputStream,conf);

        ip.close();
        outputStream.close();
    }
}
