package com.lcydream.open.project.hadooprpc.client;

import com.lcydream.open.project.hadooprpc.protocol.IUserLoginService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.net.InetSocketAddress;

/**
 * UserLoginAction
 *
 * @author Luo Chun Yun
 * @date 2018/1/7 17:08
 */
public class UserLoginAction {
    public static void main(String[] args) throws Exception{
        IUserLoginService userLoginService = RPC.getProxy(IUserLoginService.class,
                11L,
                new InetSocketAddress("localhost", 9999),
                new Configuration());
        String flag = userLoginService.login("luochunyun", "123456");
        System.out.println(flag);
    }
}


















