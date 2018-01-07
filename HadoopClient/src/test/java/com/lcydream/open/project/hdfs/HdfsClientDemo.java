package com.lcydream.open.project.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

/**
 * HdfsClientDemo
 * 客户端去操作hsfs时，是有一个用户身份的
 * 默认情况下，hsfs客户端api会从jvm中获取一个参数作为自己的身份:-DHADOOP_USER_NAME=用户名
 *
 * 也可以在构造客户端fs对象时，作为参数传递进去
 * @author Luo Chun Yun
 * @date 2017/12/23 18:17:01
 */
public class HdfsClientDemo {

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
        fs.copyFromLocalFile(new Path("D:/破解步骤.txt"),new Path("/破解.txt"));
        fs.close();
    }

    @Test
    public void testDownload() throws Exception{
        fs.copyToLocalFile(new Path("/破解.txt"),new Path("D:/"));
        fs.close();
    }

    /**
     * 打印hdfs的配置信息
     */
    @Test
    public void testConf(){
        Iterator<Map.Entry<String,String>> it = conf.iterator();
        while (it.hasNext()){
            Map.Entry<String,String> ent = it.next();
            System.out.println(ent.getKey() + " : " + ent.getValue());
        }
    }

    //打印集群的状态 hdfs dfsadmin -report
    @Test
    public void testMkdir() throws Exception{
        boolean mkdirs = fs.mkdirs(new Path("/TestMkdir/aaa/bb"));
        System.out.println(mkdirs);
    }

    @Test
    public void testDelete() throws Exception{
        boolean delete = fs.delete(new Path("/TestMkdir/aaa"), true);
        System.out.println(delete);
    }

    /**
     * 递归显示某一个目录下的文件
     * @throws Exception
     */
    @Test
    public void testLs() throws Exception{
        RemoteIterator<LocatedFileStatus> listIterator = fs.listFiles(new Path("/"), true);
        while (listIterator.hasNext()){
            LocatedFileStatus fileStatus = listIterator.next();
            System.out.println("blocksize:" + fileStatus.getBlockSize());
            System.out.println("owner:" + fileStatus.getOwner());
            System.out.println("Replication:" + fileStatus.getReplication());
            System.out.println("Permission:" + fileStatus.getPermission());
            System.out.println("Name:" + fileStatus.getPath().getName());
            System.out.println("+++++++++++++++++++++");
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for(BlockLocation b : blockLocations){
                System.out.println("块的名字：" + Arrays.asList(b.getNames()));
                System.out.println("块起始偏移量：" + b.getOffset());
                System.out.println("块长度" + b.getLength());
                System.out.println("datanode："+ Arrays.asList(b.getHosts()));
            }
        }
    }

    @Test
    public void testLs2() throws Exception{
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses){
            System.out.println("name:"+fileStatus.getPath().getName());
            System.out.println(fileStatus.isFile() ? "file" : "directory");
        }
    }
}























