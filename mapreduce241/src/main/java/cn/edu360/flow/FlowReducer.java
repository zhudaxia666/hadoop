package cn.edu360.flow;

import java.io.IOException;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/**
 *  key����ĳ���ֻ���
 *  values��������ֻ��������������з��ʼ�¼�е���������
 *  
 *  <135,flowBean1><135,flowBean2><135,flowBean3><135,flowBean4>
 */
public class FlowReducer extends Reducer<Text, FlowBeat, Text, FlowBeat>{
	protected void reduce(Text key, Iterable<FlowBeat> values, Reducer<Text,FlowBeat,Text,FlowBeat>.Context context) 
			throws IOException,InterruptedException{
		int upsum=0;
		int dsum=0;
		for(FlowBeat value:values){
			upsum+=value.getUpflow();
			dsum+=value.getDflow();
		}
		
		context.write(key, new FlowBeat(key.toString(),upsum,dsum));
	}

}
