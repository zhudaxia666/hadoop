package cn.edu360.flow;

import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
/**
 * �������ṩ��MapTask�õ�
 * MapTaskͨ��������getPartition����������������������ÿһ��kv���ݸ÷ַ�����һ��reduce task
 * @author ThinkPad
 *
 */
public class ProvicePartitioner extends Partitioner<Text, FlowBeat>{
	static HashMap<String, Integer> codemap =new HashMap<String, Integer>();
	static{
		codemap.put("135", 0);
		codemap.put("136", 1);
		codemap.put("137", 2);
		codemap.put("138", 3);
		codemap.put("139", 4);
	}

	@Override
	public int getPartition(Text key, FlowBeat value, int arg2) {
		
		Integer code = codemap.get(key.toString().substring(0,3));
		
		return code==null?5:code;
	}
	

}
