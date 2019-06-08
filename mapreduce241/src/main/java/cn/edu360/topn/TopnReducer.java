package cn.edu360.topn;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopnReducer extends Reducer<Text, IntWritable, Text, IntWritable>{


	TreeMap<topncount, Object> treeMap = new TreeMap<topncount, Object>();
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		int count=0;
		for (IntWritable value : values) {
			count+=value.get();
		}
		topncount topncount = new topncount();
		topncount.set(key.toString(),count);
		treeMap.put(topncount, null);
	}
	@Override
	protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		Configuration conf = context.getConfiguration();
		int topn = conf.getInt("top.n", 5);
		
		Set<Entry<topncount, Object>> entrySet = treeMap.entrySet();
		
		int i=0;
		for (Entry<topncount, Object> entry : entrySet) {
			context.write(new Text(entry.getKey().getPage()), new IntWritable(entry.getKey().getCount()));
			i++;
			if(i==topn)return;
			
		}
		
		
	}

}
