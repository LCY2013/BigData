package com.lcydream.open.project.hadooprpc.client;

import com.lcydream.open.project.hadooprpc.protocol.ClientNamenodeProtocol;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.net.InetSocketAddress;

/**
 * MyHdfsClient
 *
 * @author Luo Chun Yun
 * @date 2018/1/7 16:57
 */
public class MyHdfsClient {

    public static void main(String[] args) throws Exception{
        ClientNamenodeProtocol namenodeProtocol = RPC.getProxy(ClientNamenodeProtocol.class,
                1L,
                new InetSocketAddress("localhost", 8889),
                new Configuration());
        String metaData = namenodeProtocol.getMetaData("/luochunyun.avi");
        System.out.println(metaData);
    }

}






















