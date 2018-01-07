package com.lcydream.open.project.hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URI;

/**
 * HdfsStreamAccess
 *  用流的方式操作hdfs上的文件
 *  可以实现指定偏移量范围的数据
 * @author Luo Chun Yun
 * @date 2018/1/6 16:58
 */
public class HdfsStreamAccess {

    //-DHADOOP_USER_NAME=hadoop
    FileSystem fs = null;
    Configuration conf = null;

    @Before
    public void init() throws Exception{
        conf = new Configuration();
        //conf.set("fs.defaultFS","hdfs://master130:9000");
        //拿到一个文件系统操作的客户端实例对象
        //FileSystem fs = FileSystem.get(conf);
        //可以直接传入URI，和用户
        fs = FileSystem.get(new URI("hdfs://192.168.21.130:9000"),conf,"hadoop");
    }

    @Test
    public void testUpload() throws Exception{
        FSDataOutputStream outPutStream = fs.create(new Path("/luochunyun.avi"), true);
        FileInputStream fileInputStream = new FileInputStream("D:/luo.avi");

        IOUtils.copy(fileInputStream,outPutStream);
    }

    /**
     * 通过流的方式从hdfs中获取数据
     * @throws Exception
     */
    @Test
    public void testDownLoad() throws Exception{
        FSDataInputStream open = fs.open(new Path("/luochunyun.avi"));
        FileOutputStream fileOutputStream = new FileOutputStream("D:/luo.txt");
        IOUtils.copy(open,fileOutputStream);
    }
    
    
    @Test
    public void testRandomAccess() throws Exception{
        FSDataInputStream inputStream = fs.open(new Path("/luochunyun.avi"));
        inputStream.seek(12);
        FileOutputStream fileOutputStream = new FileOutputStream("D:/luo1.txt");
        IOUtils.copy(inputStream,fileOutputStream);
    }

    /**
     * 显示hdfs上文件的内容
     * @throws Exception
     */
    @Test
    public void testCat() throws Exception{
        FSDataInputStream inputStream = fs.open(new Path("/luochunyun.avi"));
        IOUtils.copy(inputStream,System.out);
        //org.apache.hadoop.io.IOUtils.copyBytes(inputStream,System.out,1024);
    }
}



































