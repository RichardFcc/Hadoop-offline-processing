package com.richard.mapreduce.clustermodel.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * @ClassName:  WordCountMapper   
 * @Description:  这是MR程序自定义Mapper
 * @author: richard
 * @Copyright: www.richard.com
 * 
 * KEYIN：表示mr程序执行时候，map阶段输入的《k,v》中的key类型
 *       在默认读取数据的组件（TextInputFormat）:一行一行的读取文件，这时候k表示每一行的起始偏移量   Long
 * 
 * VALUEIN 表示mr程序执行时候，map阶段输入的《k,v》中的value类型
 *        在默认读取数据的组件（TextInputFormat）:v就表示这一行数据     String

   KEYOUT  表示mr程序执行时候，map阶段输出的《k,v》中的key类型
                              在这里k表示每一个单词    String
   
   VALUEOUT  表示mr程序执行时候，map阶段输出的《k,v》中的value类型
           在这里单词每出现一次，标记为1    int
           
    hadoop认为jdk自带的数据类型在序列化的时候不方便，太累赘，因此自定义自己的序列化类型   Writable
    
    Long--->LongWritable
    String--->Text
    int--->intWritable
    null--->nullWriatble
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

    /*
     * 这里就是mapper阶段的具体业务逻辑
     * TextInputFormat每读取一行就会调用一次map()
     */
    @Override
    protected void map(LongWritable key, Text value,Context context)
            throws IOException, InterruptedException {
        
        //获取读取数据组件TextInputFormat传入过来的每一行内容
        String lines = value.toString();
        
        //按单词分隔符切割一行
        String[] words = lines.split(" ");
        
        for (String word:words) {
            context.write(new Text(word), new IntWritable(1));
        }
        
    }
    
}

























