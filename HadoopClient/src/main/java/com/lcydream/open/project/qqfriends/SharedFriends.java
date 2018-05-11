package com.lcydream.open.project.qqfriends;

import org.apache.commons.lang.ArrayUtils;
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
import java.util.Arrays;

/**
 * SharedFriendsStepOne
 *  求互粉的人
 * @author Luo Chun Yun
 * @date 2018/4/7 10:28
 */
public class SharedFriends {

    /**
     * 统计第一阶段的map
     */
    static class SharedFriendsStepOneMapper extends Mapper<LongWritable, Text, Text, Text> {

        /**
         *
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            //数据格式 A:B,C,D,F,E,O

            //读取数据的一行
            if(value!=null&&!"".equals(value.toString())) {
                String line = value.toString();
                //拆分数据
                String[] person_friends = line.split(":");
                //获取某个人的数据
                String person = person_friends[0].trim();
                //获取某个人的好友列表
                String[] friends = person_friends[1].split(",");
                //用一个新的数组进行排序
                String[] newfriends = new String[friends.length+1];
                //将数据拷贝到新的数组
                //newfriends = Arrays.copyOf(friends,friends.length);
                for(int i=0; i<friends.length;i++){
                    newfriends[i] = friends[i];
                }
                //将person的数据也加入都新数组用于排序
                newfriends[friends.length] = person;
                //对好友列表进行排序
                Arrays.sort(newfriends);
                //记录person在数组中的位置
                int potition = 0;
                for(int i=0;i<newfriends.length;i++){
                    if(person.equals(newfriends[i])){
                        potition = i;
                    }
                }
                //将排序好的好友列表进行阶乘式的两两配对
                for (int i=0;i<newfriends.length;i++){
                    if(i < potition){
                        context.write(new Text("<"+newfriends[i]+"-"+person+">"),new Text(person));
                    }else if(i > potition){
                        context.write(new Text("<"+person+"-"+newfriends[i]+">"),new Text(person));
                    }else {
                        continue;
                    }
                }
            }

        }

    }

    /**
     * 对数据进行合并
     */
    static class SharedFriendsStepTwoReducer extends Reducer<Text, Text, Text, NullWritable> {

        /**
         * 对数据进行合并
         * @param key
         * @param values
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            //获取这次的person-person
            String person_person = key.toString().trim();
            //用一个表示变量判断这个key是否存在有同样的key
            int i = 0;
            for(Text text : values){
                if(person_person.contains(text.toString().trim())){
                    i++;
                }
            }

            if(i == 2){
                context.write(key,NullWritable.get());
            }

        }

    }


    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(SharedFriends.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(SharedFriends.SharedFriendsStepOneMapper.class);
        job.setReducerClass(SharedFriends.SharedFriendsStepTwoReducer.class);

        FileInputFormat.setInputPaths(job, new Path("D:/hadoopwork/qqfriendsinput"));
        FileOutputFormat.setOutputPath(job, new Path("D:/hadoopwork/qqfriendsoutput"));

        job.waitForCompletion(true);

    }
}
