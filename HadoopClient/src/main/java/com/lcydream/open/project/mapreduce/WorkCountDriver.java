package com.lcydream.open.project.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * WorkCountDriver
 * 相当于一个yarn集群的客户端
 *  需要在此封装我们的mr程序的相关参数，指定jar包
 *  最后提交yarn
 * @author Luo Chun Yun
 * @date 2018/1/20 18:05
 */
public class WorkCountDriver {

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        /*本地模式启动配置,默认就是本地运行模式*/
        /*conf.set("mapreduce.framework.name","local");
        conf.set("fs.defaultFS","file:///");*/

        //运行集群模式，就是把程序提交到yarn中去运行
        //要想运行为集群模式，以下三个参数要指定集群上的值
        /*conf.set("mapreduce.framework.name","yarn");
        conf.set("yarn.resoucemanager.hostname","master130");
        conf.set("fs.defaultFS"."hdfs://master130:9000/")*/
        Job job = Job.getInstance(conf);

        job.setJar("D:\\SoftWareTools\\idea2017\\ideaWorkPace\\BigData\\HadoopClient\\target\\wc.jar");
        //job.setJar("/home/hadoop/wc.jar");
        //指定本程序的jar包所在的本地路径
        //job.setJarByClass(WorkCountDriver.class);

        //指定本业务job要使用的mapper/Reducer业务类
        job.setMapperClass(WorkCountMapper.class);
        job.setReducerClass(WorkCountReduce.class);

        //指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //指定最终输出的数据的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //指定需要使用combiner，以及那个combiner的业务逻辑
        job.setCombinerClass(WordcountCombiner.class);

        //如果不设置inputformat，默认使用的是TextInputformat.class
        /*job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMaxInputSplitSize(job,4194304);
        CombineTextInputFormat.setMinInputSplitSize(job,2097152);*/

        /*//指定job的输入原始文件所在目录
        FileInputFormat.setInputPaths(job,new Path("/wordcount/input"));
        //指定job的输出结果所在目录
        FileOutputFormat.setOutputPath(job,new Path("/workcount/output"));*/

        //指定job的输入原始文件所在目录
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        //指定job的输出结果所在目录
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //将job中配置的相关参数，以及job所用的java类所在的jar包，提交给yarn去运行
        /*job.submit();*/
        boolean completion = job.waitForCompletion(true);
        System.exit(completion?0:1);
    }
}
