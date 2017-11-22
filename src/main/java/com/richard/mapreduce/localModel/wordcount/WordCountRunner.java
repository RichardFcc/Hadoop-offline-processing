package com.richard.mapreduce.localModel.wordcount;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @ClassName: WordCountRunner
 * @Description: 这里是mr程序运行的主类所在：告诉框架本次mr程序的相关信息（组装）
 * @author: Allen Woon
 * @date: 2017年11月1日 下午7:30:29
 * @Copyright: www.itcast.cn
 */
public class WordCountRunner {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        conf.set("mapreduce.framework.name", "local");

        //通过Job来封装本次mr的相关信息
        Job job = Job.getInstance(conf);

        //指定本次jobjar包运行的主类
        job.setJarByClass(WordCountRunner.class);

        //指定本次job 所用的mapper类 reduce类分别是谁
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //指定本次jobmapper输出的k v类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //指定本次job最终的输出k v类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(2);
        //指定本次job待处理数据所在hdfs上的目录
        FileInputFormat.setInputPaths(job, "D:\\testcode\\input");
        //指定本次job输出结果存放的路劲
        FileOutputFormat.setOutputPath(job, new Path("D:\\testcode\\output"));


//       job.submit();
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }

}















