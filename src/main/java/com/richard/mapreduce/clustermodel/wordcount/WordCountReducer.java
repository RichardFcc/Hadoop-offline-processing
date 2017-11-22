package com.richard.mapreduce.clustermodel.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/**
 * @ClassName:  WordCountReducer   
 * @Description: 这里是mr自定义的Reduce
 * @author: richard
 * @Copyright: www.richard.com
 * 
 * <>
 * 
 * KEYIN    就是mapper阶段输出的key   在这里就是单词：  Text
 * VALUEIN  就是mapper阶段输出的value  这里就是标记的单词出现次数：1  IntWritable
 * 
 * KEYOUT    就是reduce阶段的输出 ，也是整个mr程序的最终输出key    在这里就是单词：  Text
 * VALUEOUT  就是reduce阶段的输出 ，也是整个mr程序的最终输出value  最终的单词总次数   IntWritable
 * 
 * 
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
    
    
    //<hello,1> <tom,1> <allen,1><hello,1><itcast,1><allen,1><allen,1>
    
    //reduce接收数据后要排序  按照key字典序
    //<allen,1><allen,1><allen,1><hello,1><hello,1><itcast,1><tom,1>
    //  <hello[1 1]> 按照key是否相同 ，相同的一组作为输入传入的reduce中
    //这个<allen,[1 1 1]>时候key：就是这一组单词的key  value：这一组单词的所有value组成的迭代器
    @Override
    protected void reduce(Text key, Iterable<IntWritable> valus,Context context)
            throws IOException, InterruptedException {
            //定义一个计数器
            int count = 0;
        
            //遍历一组迭代器，把每一个数累加  [1 1 1]
            for (IntWritable value : valus) {
                count += value.get();
            }
        
           //把本地调用计算结果输出
            context.write(key, new IntWritable(count));
            //默认有自己的数据输出组件TextOutPutFormat
    }
}











