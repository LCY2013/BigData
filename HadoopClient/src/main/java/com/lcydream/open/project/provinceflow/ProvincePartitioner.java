package com.lcydream.open.project.provinceflow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

/**
 * ProvincePartitioner
 *  k  v  对应的是map输出对应的kv类型
 * @author Luo Chun Yun
 * @date 2018/3/6 20:28
 */
public class ProvincePartitioner extends Partitioner<Text,FlowBean> {

    public static HashMap<String,Integer> proviceDict = new HashMap<String,Integer>();
    static {
        proviceDict.put("136",0);
        proviceDict.put("137",1);
        proviceDict.put("138",2);
        proviceDict.put("139",3);
    }

    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {

        String prefix = text.toString().substring(0, 3);
        Integer provinceId = proviceDict.get(prefix);

        return provinceId==null?4:provinceId;
    }

}















