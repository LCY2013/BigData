package com.lcydream.open.project.hadooprpc.service;

import com.lcydream.open.project.hadooprpc.protocol.IUserLoginService;

/**
 * UserLoginServiceImpl
 *
 * @author Luo Chun Yun
 * @date 2018/1/7 17:09
 */
public class UserLoginServiceImpl implements IUserLoginService {

    @Override
    public String login(String name, String passwd) {
        return name + "login success .....";
    }
}

















