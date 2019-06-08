package cn.edu360.count;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageCountstep2 {
	
	public static class PageCountstep2Mapper extends Mapper<LongWritable, Text, PageCount, NullWritable>{
		protected void map(LongWritable key, Text value, Mapper<LongWritable,Text,PageCount,NullWritable>.Context context) 
				throws java.io.IOException ,InterruptedException {
			
			String line = value.toString();
			String[] split = line.split("\t");
			PageCount pageCount = new PageCount();
			pageCount.set(split[0], Integer.parseInt(split[1]));
			
			context.write(pageCount, NullWritable.get());
		};
		
	}
	public static class PageCountstep2Reducer extends Reducer<PageCount, NullWritable, PageCount, NullWritable>{
		
		@Override
		protected void reduce(PageCount key, Iterable<NullWritable> values,
				Reducer<PageCount, NullWritable, PageCount, NullWritable>.Context context)
				throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
		}
		
	}

	public static void main(String[] args) throws Exception {
		 Configuration conf = new Configuration();
			
			Job job = Job.getInstance(conf);

			job.setJarByClass(PageCountstep2.class);

			job.setMapperClass(PageCountstep2Mapper.class);
			job.setReducerClass(PageCountstep2Reducer.class);

			job.setMapOutputKeyClass(PageCount.class);
			job.setMapOutputValueClass(NullWritable.class);
			
			job.setOutputKeyClass(PageCount.class);
			job.setOutputValueClass(NullWritable.class);

			FileInputFormat.setInputPaths(job, new Path("E:\\hadoop_data\\url\\output_count"));
			FileOutputFormat.setOutputPath(job, new Path("E:\\hadoop_data\\url\\output_sort"));

			job.setNumReduceTasks(1);
			
			job.waitForCompletion(true);
			

	}

}
