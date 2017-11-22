package com.richard.mapreduce.flowsumsort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class FlowBean implements WritableComparable<FlowBean>{
    
    
    private long upFlow;
    
    private long downFlow;
    
    private long sumFlow;
    
    //这里反序列的时候会用到
    public FlowBean() {
    }

    public FlowBean(long upFlow, long downFlow, long sumFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = sumFlow;
    }
    
    public FlowBean(long upFlow, long downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow+downFlow;
    }
    
    public void set(long upFlow, long downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow+downFlow;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }
    

    @Override
    public String toString() {
        return upFlow+"\t"+downFlow+"\t"+sumFlow;
    }

    /**
     * 这里是序列化方法
     * <p>Title: write</p>   
     * <p>Description: </p>   
     * @param out
     * @throws IOException   
     * @see Writable#write(DataOutput)
     */
    @Override
    public void write(DataOutput out) throws IOException {
          out.writeLong(upFlow);
          out.writeLong(downFlow);
          out.writeLong(sumFlow);
    }

    /**
     * 这里是反序列化方法
     * <p>Title: readFields</p>   
     * <p>Description: </p>   
     * @param in
     * @throws IOException   
     * @see Writable#readFields(DataInput)
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        //注意反序列化的顺序跟序列化的顺序一致
       this.upFlow = in.readLong();
       this.downFlow = in.readLong();
       this.sumFlow = in.readLong();
        
    }

    //这里进行bean的自定义比较大小
    @Override
    public int compareTo(FlowBean o) {
        //实现按照 sumflow 的大小倒序排序
        return this.sumFlow>o.getSumFlow()?-1:1;
    }
    
    

}
