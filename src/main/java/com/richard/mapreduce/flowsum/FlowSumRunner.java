package com.richard.mapreduce.flowsum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlowSumRunner {
    public static void main(String[] args) throws Exception{
 Configuration conf = new Configuration();
        
        conf.set("mapreduce.framework.name", "local");
        
        //通过Job来封装本次mr的相关信息
        Job job =Job.getInstance(conf);
        
        //指定本次jobjar包运行的主类
        job.setJarByClass(FlowSumRunner.class);
        
        //指定本次job 所用的mapper类 reduce类分别是谁
        job.setMapperClass(FlowSumMapper.class);
        job.setReducerClass(FlowSumReducer.class);
        
        //指定本次jobmapper输出的k v类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        
        //指定本次job最终的输出k v类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        
//        job.setNumReduceTasks(3);
        
        //指定本次job待处理数据所在hdfs上的目录
        FileInputFormat.setInputPaths(job, "D:\\textcode\\flowsum\\input");
        //指定本次job输出结果存放的路劲
        FileOutputFormat.setOutputPath(job, new Path("D:\\textcode\\flowsum\\output"));
        
        
        
//       job.submit();
         boolean b = job.waitForCompletion(true);
         System.exit(b?0:1);
    }
}
