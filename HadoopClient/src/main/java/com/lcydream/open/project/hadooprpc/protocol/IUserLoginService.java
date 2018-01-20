package com.lcydream.open.project.hadooprpc.protocol;

/**
 * UserLoginService
 *
 * @author Luo Chun Yun
 * @date 2018/1/7 17:09
 */
public interface IUserLoginService {
    public static final long versionID = 11L;
    public String login(String name,String passwd);
}
