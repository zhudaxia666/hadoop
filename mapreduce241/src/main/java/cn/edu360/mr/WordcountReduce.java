package cn.edu360.mr;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordcountReduce extends Reducer<Text, IntWritable, Text, IntWritable>{
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,Context context) 
			throws IOException, InterruptedException {
		int count=1;
		
		Iterator<IntWritable> iterator = values.iterator();//���õ�������
		while(iterator.hasNext()){
			IntWritable value = iterator.next();
			count+=value.get();
		}
		context.write(key, new IntWritable(count));
		//��������Ҫ����һЩ��������maptask֪�����Ǹ��ࡣreducetask����һ����д����ʵ��
		//������Щ��������maptask����Щ����reducetask����Ҫһ��ƽ̨���Ȼ��ơ����ƽ̨����yarn
		
	}

}
