package com.lcydream.open.project.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * WorkCountMapper
 *
 * KEYIN:默认情况下，是mr框架所读到的一行文本的起始偏移量，Long，
 *       但是在hadoop中有自己更精简的序列化接口，所以不直接用Long，而是用LongWritable
 * VALUEIN:默认情况下是mr框架所读到的一行文本的内容，String，同上，用Text
 *
 * KEYOUT:是用户自定义逻辑处理完成后输出数据中的key，在此处是单词，string
 * VALUEOUT:是用户自定义逻辑处理完成后输出数据中的value，在此处是单词次数，Integer,同上用IntWritable
 *
 * @author Luo Chun Yun
 * @date 2018/1/20 17:13
 */
public class WorkCountMapper extends Mapper<LongWritable,Text,Text,IntWritable>{

    /**
     * map阶段的业务逻辑就写在自定义的map()方法中
     * maptask会对每一行输入数据调用一次我们自定义的map()方法
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //将maptask传给我们的文本内容先转换成String
        String line = value.toString();
        //根据空格将这一行切分成单词
        String[] words = line.split("");

        //将单词输出为<单词,1>
        for(String word : words){
            //将单词作为key，将次数1作为value，以便于后续的数据分发，可以根据单词分发，以便于相同的单词回到相同的reduce task中
            context.write(new Text(word),new IntWritable(1));
        }


    }
}





















