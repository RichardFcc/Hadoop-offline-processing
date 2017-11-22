package com.richard.mapreduce.flowsumsort.province;

import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


public class ProvincePartitioner extends Partitioner<Text, FlowBean> {
    
    
    public static HashMap<String, Integer>  provinceMap = new HashMap<String, Integer>();
    //实际开发中，要和jdbc联系，从数据库取出
    //实际开发中，不仅就只要0-4这五个分区，还会涉及other这个一个分区
    static{
        provinceMap.put("134", 0);
        provinceMap.put("135", 1);
        provinceMap.put("136", 2);
        provinceMap.put("137", 3);
        provinceMap.put("138", 4);
    }
    
    //key就是手机号码
    @Override
    public int getPartition(Text key, FlowBean value, int numPartitions) {

        Integer code = provinceMap.get(key.toString().substring(0, 3));
        
        if (code != null) {
            return code;
        }
        
        return 5;
    }

}
