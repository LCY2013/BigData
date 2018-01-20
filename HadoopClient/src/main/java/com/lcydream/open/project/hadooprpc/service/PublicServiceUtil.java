package com.lcydream.open.project.hadooprpc.service;

import com.lcydream.open.project.hadooprpc.protocol.ClientNamenodeProtocol;
import com.lcydream.open.project.hadooprpc.protocol.IUserLoginService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

/**
 * PublicServiceUtil
 *
 * @author Luo Chun Yun
 * @date 2018/1/7 16:47
 */
public class PublicServiceUtil {

    public static void main(String[] args) throws Exception{
        RPC.Builder builder = new RPC.Builder(new Configuration());
        builder.setBindAddress("localhost")
        .setPort(8889)
        .setProtocol(ClientNamenodeProtocol.class)
        .setInstance(new MyNameNode());

        RPC.Server server = builder.build();
        server.start();

        RPC.Builder builder2 = new RPC.Builder(new Configuration());
        builder2.setBindAddress("localhost")
                .setPort(9999)
                .setProtocol(IUserLoginService.class)
                .setInstance(new UserLoginServiceImpl());

        RPC.Server server2 = builder2.build();
        server2.start();
    }
}

















