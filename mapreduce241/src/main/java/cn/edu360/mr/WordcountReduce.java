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
		
		Iterator<IntWritable> iterator = values.iterator();//先拿到迭代器
		while(iterator.hasNext()){
			IntWritable value = iterator.next();
			count+=value.get();
		}
		context.write(key, new IntWritable(count));
		//接下来还要设置一些参数，让maptask知道掉那个类。reducetask叼拿一个，写程序实现
		//到底哪些机器启动maptask，哪些启动reducetask，需要一个平台调度机制。这个平台就是yarn
		
	}

}
