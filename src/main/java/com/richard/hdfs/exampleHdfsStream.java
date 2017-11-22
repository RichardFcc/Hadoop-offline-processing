package com.richard.hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URI;

public class exampleHdfsStream {

        FileSystem fs = null;
        Configuration conf =null;
        @Before
        public void init() throws Exception{
            conf = new Configuration();
            fs = FileSystem.get(new URI("hdfs://node-1:9000"), conf, "root");
        }
        @Test
        public void testUpload() throws Exception {
            FSDataInputStream inputStream = fs.open(new Path("/1.txt"));
            FileOutputStream outputStream = new FileOutputStream("D:\\testcode\\1.txt");

            IOUtils.copy(inputStream, outputStream);
        }

    }
