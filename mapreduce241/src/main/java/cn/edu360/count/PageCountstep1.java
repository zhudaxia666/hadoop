package cn.edu360.count;

import java.io.IOException;

import javax.sound.midi.Patch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageCountstep1 {
	
	public static class PageCountstep1Mapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		protected void map(LongWritable key, Text value, Mapper<LongWritable,Text,Text,IntWritable>.Context context) 
				throws java.io.IOException ,InterruptedException {
			
			String line = value.toString();
			String[] split = line.split(" ");
			context.write(new Text(split[1]), new IntWritable(1));
		};
		
	}
	public static class PageCountstep1reducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			int count=0;
			for (IntWritable value : values) {
				count+=value.get();
			}
			context.write(key, new IntWritable(count));
		}
		
	}

	public static void main(String[] args) throws Exception {
		Configuration configuration =new Configuration();
		
		Job job = Job.getInstance(configuration);
		
		job.setJarByClass(PageCountstep1.class);
		
		job.setMapperClass(PageCountstep1Mapper.class);
		job.setReducerClass(PageCountstep1reducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path("E:\\hadoop_data\\url\\input"));
		FileOutputFormat.setOutputPath(job, new Path("E:\\hadoop_data\\url\\output_count"));
		
		job.setNumReduceTasks(3);
		
		job.waitForCompletion(true);

	}

}
