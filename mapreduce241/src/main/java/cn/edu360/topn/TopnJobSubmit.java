package cn.edu360.topn;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopnJobSubmit {

	public static void main(String[] args) throws Exception, Exception {
		
		/**
		 * 通过加载classpath下的*-site.xml文件解析参数
		 */
		Configuration conf = new Configuration();
		conf.addResource("xx-oo.xml");
		
		/**
		 * 通过代码设置参数
		 */
		//conf.setInt("top.n", 3);
		//conf.setInt("top.n", Integer.parseInt(args[0]));
		
		/**
		 * 通过属性配置文件获取参数
		 */
		/*Properties props = new Properties();
		props.load(JobSubmitter.class.getClassLoader().getResourceAsStream("topn.properties"));
		conf.setInt("top.n", Integer.parseInt(props.getProperty("top.n")));*/
		
		Job job = Job.getInstance(conf);

		job.setJarByClass(TopnJobSubmit.class);

		job.setMapperClass(TopnMapper.class);
		job.setReducerClass(TopnReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.setInputPaths(job, new Path("E:\\hadoop_data\\url\\input"));
		FileOutputFormat.setOutputPath(job, new Path("E:\\hadoop_data\\url\\output"));

		job.waitForCompletion(true);

	}

}
