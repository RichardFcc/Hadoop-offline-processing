package com.richard.mapreduce.flowsum;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 *我们也可以在kv中传入自定义的数据，前提是实现Hadoop序列化机制
 */
public class FlowSumMapper extends Mapper<LongWritable,Text,Text,FlowBean>{
   Text k = new Text();
   FlowBean v = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();

        String[] fileds = line.split("\t");

        String phoneNum = fileds[1];
        //设置上行流量
        long upFlow =Long.parseLong(fileds[fileds.length-3]);
        //设置下行流量
        long downFlow =Long.parseLong(fileds[fileds.length-2]);
        System.out.println(upFlow);

        //在MapTask阶段对数据进行封装
          //数据量大的情况下new对象不方便context.write(new Text(phoneNum),new FlowBean(upFlow,downFlow));

        //在FlowBean中自定义一个set构造方法
        k.set(phoneNum);
        //在FlowBean中自定义一个set构造方法
        v.set(upFlow,downFlow);

        context.write(k,v);
    }
}
