package cn.edu360.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JobSubmitWindowsLocal {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		//conf.set("fs.defaultFS", "file:///");
		//conf.set("mapreduce.framework.name", "local");

		Job job = Job.getInstance(conf);
		job.setJarByClass(JobSubmitterLinuxtoYarn.class);
		
		job.setMapperClass(WordcountMapper.class);
		job.setReducerClass(WordcountReduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//设置maptask端的局部混合逻辑类
		job.setCombinerClass(WordCombiner.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path("E:\\hadoop_data\\index\\input"));
		FileOutputFormat.setOutputPath(job, new Path("E:\\hadoop_data\\index\\output4"));
		
		job.setNumReduceTasks(3);
		
		boolean res = job.waitForCompletion(true);
		System.exit(res?0:1);

	}

}
