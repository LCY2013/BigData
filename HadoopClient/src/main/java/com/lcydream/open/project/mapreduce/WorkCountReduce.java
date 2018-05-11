package com.lcydream.open.project.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * WorkCountReduce
 * KEYIN,VALUEIN对应mapper输出的KEYOUT，VALUEOUT类型对应
 * KEYOUT，VALUEOUT 是自定义reduce逻辑处理结果的输出数据类型
 * KEYOUT是单词
 * VALUEOUT是总次数
 *
 * @author Luo Chun Yun
 * @date 2018/1/20 17:36
 */
public class WorkCountReduce extends Reducer<Text,IntWritable,Text,IntWritable>{

    /**
     * <my,1>
     * <hi,1>
     * 入参key，是一组相同单词kv对的key
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        /*Iterator<IntWritable> iterator = values.iterator();
        while (iterator.hasNext()){
            count += iterator.next().get();
        }*/

        for(IntWritable value : values){
            count += value.get();
        }

        context.write(key,new IntWritable(count));
    }
}
















