package cn.edu360.friend;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CommonFriend1 {
	
	public static class CommonFriend1Mapper extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			String[] fields = value.toString().split("\t");
			context.write(new Text(fields[0]), new Text(fields[1]));
			
		}
		
	}
	public static class CommonFriend1Reducer extends Reducer<Text, Text, Text, Text> {
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// stringbuffer是线程安全的，stringbuilder是非线程安全的，在不涉及线程安全的场景下，stringbuilder更快
			StringBuilder sb = new StringBuilder();
			for (Text value : values) {
				sb.append(value.toString()).append("\t");
			}
			context.write(key, new Text(sb.toString()));
		}
		
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(CommonFriend1.class);
		
		job.setMapperClass(CommonFriend1Mapper.class);
		job.setReducerClass(CommonFriend1Reducer.class);
		
		job.setNumReduceTasks(2);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path("E:\\hadoop_data\\friends\\output"));
		FileOutputFormat.setOutputPath(job, new Path("E:\\hadoop_data\\friends\\output1"));
		
		job.waitForCompletion(true);
	}

}
