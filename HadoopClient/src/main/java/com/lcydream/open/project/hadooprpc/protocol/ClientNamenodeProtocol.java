package com.lcydream.open.project.hadooprpc.protocol;

/**
 * ClientNamenodeProtocol
 *
 * @author Luo Chun Yun
 * @date 2018/1/7 16:49
 */
public interface ClientNamenodeProtocol {
    public static final long versionID = 1L;
    public String getMetaData(String path);
}
