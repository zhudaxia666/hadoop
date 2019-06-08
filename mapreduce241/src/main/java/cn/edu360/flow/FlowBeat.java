package cn.edu360.flow;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
/**
 * �������Ĺ��ܣ���ʾ�Զ��������������ʵ��hadoop�����л��ӿ�
 * 1������һ��Ҫ�����ղι��캯��
 * 2��write����������ֶζ��������ݵ�˳��  Ҫ��  readFields������ȡ���ݵ�˳��һ��
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
	//hadoopϵͳ�ڷ����л�����Ķ���ʱҪ���õķ���
	public void readFields(DataInput in) throws IOException {
		this.upflow=in.readInt();
		this.dflow=in.readInt();
		this.phone=in.readUTF();
		this.amount=in.readInt();
		
	}
	//hadoopϵͳ�����л�����Ķ�����Ҫ���õķ���
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
