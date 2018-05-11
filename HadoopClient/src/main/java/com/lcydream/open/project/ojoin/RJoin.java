package com.lcydream.open.project.ojoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.springframework.beans.BeanUtils;

import java.io.IOException;
import java.util.ArrayList;

/**
 * RJoin
 * 订单表和商品表合到一起
 order.txt(订单id, 日期, 商品编号, 数量)
 1001	20150710	P0001	2
 1002	20150710	P0001	3
 1002	20150710	P0002	3
 1003	20150710	P0003	3
 product.txt(商品编号, 商品名字, 价格, 数量)
 P0001	小米5	1001	2
 P0002	锤子T1	1000	3
 P0003	锤子	1002	4
 * @author Luo Chun Yun
 * @date 2018/4/6 10:49
 */
public class RJoin {

    static class RJoinMapper extends Mapper<LongWritable,
            Text,Text,InfoBean> {

        InfoBean infoBean = new InfoBean();
        Text text = new Text();

        @Override
        protected void map(LongWritable key, Text value,
                           Context context) throws IOException, InterruptedException {
            //获取文件的每行
            String line = value.toString();

            //获取这个操作的文件的分块
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            //获取这次操作的文件名称
            String fileName = fileSplit.getPath().getName();
            //通过文件名判断是那种数据
            String pid = "";
            if(fileName.startsWith("order")){
                String[] splits = line.split("\t");
                //id data pid amount
                pid = splits[2];
                infoBean.set(Integer.parseInt(splits[0]), splits[1], pid, Integer.parseInt(splits[3]), "", 0, 0, "0");
            }else {
                String[] splits = line.split("\t");
                //id data pid amount
                pid = splits[0];
                infoBean.set(0, "", pid, 0, splits[1], Integer.parseInt(splits[2]), Float.parseFloat(splits[3]), "1");
            }
            text.set(pid);
            context.write(text,infoBean);
        }
    }


    static class RJoinReduce extends Reducer<Text, InfoBean, InfoBean, NullWritable> {

        @Override
        protected void reduce(Text key, Iterable<InfoBean> values,
                              Context context) throws IOException, InterruptedException {
            InfoBean pdBean = new InfoBean();
            ArrayList<InfoBean> orderBeans = new ArrayList<InfoBean>();

            //根据bean的类型判断到时那种bean，产品就直接复制到主InfoBean中
            for (InfoBean infoBean : values){
                //产品bean
                if("1".equals(infoBean.getFlag().trim())){
                    BeanUtils.copyProperties(infoBean,pdBean);
                }else {
                    InfoBean orderInfoBean = new InfoBean();
                    BeanUtils.copyProperties(infoBean,orderInfoBean);
                    orderBeans.add(orderInfoBean);
                }
            }

            //合并完整的bean对象
            for (InfoBean bean : orderBeans){
                bean.setPname(pdBean.getPname());
                bean.setCategory_id(pdBean.getCategory_id());
                bean.setPrice(pdBean.getPrice());
                context.write(bean,NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        /*conf.set("mapred.textoutputformat.separator", "\t");*/

        Job job = Job.getInstance(conf);

        // 指定本程序的jar包所在的本地路径
        // job.setJarByClass(RJoin.class);
		//job.setJar("D:\\SoftWareTools\\idea2017\\ideaWorkPace\\BigData\\HadoopClient\\target\\wc.jar");
		job.setJar("D:\\SoftWareTools\\idea2017\\ideaWorkPace\\BigData\\HadoopClient\\target\\wc-jar-with-dependencies.jar");
        //job.setJar("/home/hadoop/wc.jar");

        //job.setJarByClass(RJoin.class);
        // 指定本业务job要使用的mapper/Reducer业务类
        job.setMapperClass(RJoinMapper.class);
        job.setReducerClass(RJoinReduce.class);

        // 指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(InfoBean.class);

        // 指定最终输出的数据的kv类型
        job.setOutputKeyClass(InfoBean.class);
        job.setOutputValueClass(NullWritable.class);

        // 指定job的输入原始文件所在目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        // 指定job的输出结果所在目录
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 将job中配置的相关参数，以及job所用的java类所在的jar包，提交给yarn去运行
        /* job.submit(); */
        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);

    }

}











