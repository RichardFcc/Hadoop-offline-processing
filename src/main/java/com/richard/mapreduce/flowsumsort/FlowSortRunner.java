package com.richard.mapreduce.flowsumsort;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by s on 2017/11/2.
 */
public class FlowSortRunner {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "local");
        Job job = Job.getInstance(conf);

        job.setJarByClass(FlowSortRunner.class);

        job.setMapperClass(FlowSortMapper.class);
        job.setReducerClass(FlowSortReducer.class);

        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        FileInputFormat.setInputPaths(job,"D:\\textcode\\flowsumsort\\input");
        FileOutputFormat.setOutputPath(job,new Path("D:\\textcode\\flowsumsort\\output"));

        // 向 yarn 集群提交这个 job
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
