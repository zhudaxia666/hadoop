package cn.edu360.index;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class IndexStepOne {
	
	public static class IndexStepOneMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		// ���� <hello-�ļ�����1> 
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// ��������Ƭ��Ϣ�л�ȡ��ǰ���ڴ����һ�������������ļ�
			FileSplit inputSplit = (FileSplit) context.getInputSplit();
			String fileName = inputSplit.getPath().getName();
			String[] words = value.toString().split(" ");
			for (String w : words) {
				// ��"����-�ļ���"��Ϊkey��1��Ϊvalue�����
				context.write(new Text(w+'-'+fileName), new IntWritable(1));
			}
		}
	}
	public static class IndexStepOneReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
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
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(IndexStepOne.class);
		
		job.setMapperClass(IndexStepOneMapper.class);
		job.setReducerClass(IndexStepOneReducer.class);
		
		job.setNumReduceTasks(3);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path("E:\\hadoop_data\\index\\input"));
		FileOutputFormat.setOutputPath(job, new Path("E:\\hadoop_data\\index\\output"));
		
//		job.setNumReduceTasks(2);
		
		
		job.waitForCompletion(true);
	}
	
	

}
