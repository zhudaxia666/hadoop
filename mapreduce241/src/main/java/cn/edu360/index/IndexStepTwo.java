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


public class IndexStepTwo {
	
	public static class IndexStepTwoMapper extends Mapper<LongWritable, Text, Text, Text>{
		//������� ƫ����  hello-a.txt 3... 
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split("-");
			context.write(new Text(split[0]), new Text(split[1].replaceAll("\t", "-->")));
			
		}
	}
	public static class IndexStepTwoReducer extends Reducer<Text, Text, Text, Text>{
		// һ�����ݣ�  <hello,a.txt-->4> <hello,b.txt-->4> <hello,c.txt-->4>����������������ʵ����ӣ��ַ������
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			// stringbuffer���̰߳�ȫ�ģ�stringbuilder�Ƿ��̰߳�ȫ�ģ��ڲ��漰�̰߳�ȫ�ĳ����£�stringbuilder����
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
		
		job.setJarByClass(IndexStepTwo.class);
		
		job.setMapperClass(IndexStepTwoMapper.class);
		job.setReducerClass(IndexStepTwoReducer.class);
		
		job.setNumReduceTasks(3);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path("E:\\hadoop_data\\index\\output"));
		FileOutputFormat.setOutputPath(job, new Path("E:\\hadoop_data\\index\\out1put"));
		
//		job.setNumReduceTasks(2);
		
		
		job.waitForCompletion(true);
	}
	
	

}
