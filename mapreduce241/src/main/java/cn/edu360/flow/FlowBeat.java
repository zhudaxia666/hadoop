package cn.edu360.flow;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
/**
 * 本案例的功能：演示自定义数据类型如何实现hadoop的序列化接口
 * 1、该类一定要保留空参构造函数
 * 2、write方法中输出字段二进制数据的顺序  要与  readFields方法读取数据的顺序一致
 * 
 * @author ThinkPad
 *
 */
public class FlowBeat implements Writable{
	
	private int upflow;
	private int dflow;
	private int amount;
	private String phone;
	
	public FlowBeat() {}
	public FlowBeat(String phone,int upflow,int dflow){
		this.upflow=upflow;
		this.dflow=dflow;
		this.phone=phone;
		this.amount=upflow+dflow;
	}
	

	public int getUpflow() {
		return upflow;
	}

	public void setUpflow(int upflow) {
		this.upflow = upflow;
	}

	public int getDflow() {
		return dflow;
	}

	public void setDflow(int dflow) {
		this.dflow = dflow;
	}

	public int getAmount() {
		return amount;
	}

	public void setAmount(int amount) {
		this.amount = amount;
	}

	public String getPhone() {
		return phone;
	}

	public void setPhone(String phone) {
		this.phone = phone;
	}
	//hadoop系统在反序列化该类的对象时要调用的方法
	public void readFields(DataInput in) throws IOException {
		this.upflow=in.readInt();
		this.dflow=in.readInt();
		this.phone=in.readUTF();
		this.amount=in.readInt();
		
	}
	//hadoop系统在序列化该类的对象是要调用的方法
	public void write(DataOutput out) throws IOException {
		out.writeInt(upflow);
		out.writeInt(dflow);
		out.writeUTF(phone);
		out.writeInt(amount);
		
	}
	@Override
	public String toString() {
		return this.upflow+","+this.dflow+","+this.amount;
	}

}
