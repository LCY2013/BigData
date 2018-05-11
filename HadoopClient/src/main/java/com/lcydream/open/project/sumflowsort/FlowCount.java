package com.lcydream.open.project.sumflowsort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * FlowCount
 *
 * @author Luo Chun Yun
 * @date 2018/2/25 10:35
 */
public class FlowCount {

    static class FlowCountMapper extends Mapper<LongWritable,Text, FlowBean,NullWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //将一行内容转成String
            String line = value.toString();
            //切分字段
            String[] fields = line.split("\t");
            //取出手机号码
            String phoneNb = fields[0];
            //取出上行流量下行流量
            long upFlow = Long.parseLong(fields[1]);
            long downFlow = Long.parseLong(fields[2]);
            System.out.println(phoneNb+":"+upFlow+":"+downFlow);
            context.write(new FlowBean(phoneNb,upFlow,downFlow),NullWritable.get());
        }
    }

    static class FlowCountReducer extends Reducer<FlowBean,NullWritable,Text,FlowBean>{
        @Override
        protected void reduce(FlowBean fw, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            String phoneNB = fw.getPhoneNB();
            context.write(new Text(phoneNB),fw);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        /*job.setJar("/home/hadoop/wc.jar");*/
        //指定本程序的jar包所在的本地路径
        job.setJarByClass(FlowCount.class);

        //指定本业务job要使用的mapper/Reducer业务类
        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReducer.class);

        //指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        //指定最终输出的数据的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //定义自己的数据分区器
        job.setPartitionerClass(ProvincePartitioner.class);

        //指定相应"分区"数量的reducetask
        job.setNumReduceTasks(5);

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
