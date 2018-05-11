package com.lcydream.open.project.sumflowsort;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * FlowBean
 *
 * @author Luo Chun Yun
 * @date 2018/2/25 10:39
 */
public class FlowBean implements Writable,WritableComparable<FlowBean>{

    private String phoneNB;
    private long upFlow;
    private long downFlow;
    private long sumFlow;

    //反序列化时，需要反射调用空构造函数，所以要显示定义一个
    public FlowBean() {
    }

    public FlowBean(String phoneNB,long upFlow, long downFlow){
        this.phoneNB = phoneNB;
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
    }

    public String getPhoneNB() {
        return phoneNB;
    }

    public void setPhoneNB(String phoneNB) {
        this.phoneNB = phoneNB;
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

    /**
     * 序列化方法
     * @param dataOutput
     * @throws IOException
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(phoneNB.trim());
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(sumFlow);
    }

    /**
     * 反序列化方法
     * 反序列化的顺序与序列化的顺序一致
     * @param dataInput
     * @throws IOException
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.phoneNB = dataInput.readUTF();
        this.upFlow = dataInput.readLong();
        this.downFlow = dataInput.readLong();
        this.sumFlow = dataInput.readLong();
    }

    @Override
    public String toString() {
        return "" + upFlow + "\t" + upFlow + "\t" + upFlow;
    }

    @Override
    public int compareTo(FlowBean o) {
        return this.sumFlow>o.sumFlow?-1:1;
    }
}
