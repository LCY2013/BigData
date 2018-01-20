package com.lcydream.open.project.hadooprpc.service;

import com.lcydream.open.project.hadooprpc.protocol.ClientNamenodeProtocol;

/**
 * MyNameNode
 *
 * @author Luo Chun Yun
 * @date 2018/1/7 16:43
 */
public class MyNameNode implements ClientNamenodeProtocol{

    //模拟namenode的业务方法之一，查询元数据
    public String getMetaData(String path){
      return path + ": 3 - {BLK_1,BLK_2} ...";
    }
}















