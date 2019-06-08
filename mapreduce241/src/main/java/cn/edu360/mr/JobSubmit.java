package cn.edu360.mr;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class JobSubmit {
	public static void main(String[] args) throws Exception {
		
		// 在代码中设置JVM系统参数，用于给job对象来获取访问HDFS的用户身份。
		//也可以在run configuration中设置参数-DHADOOP_USER_NAME=root
		System.setProperty("HADOOP_USER_NAME", "root");
		
		Configuration conf = new Configuration();
		//1.设置job运行时要访问的默认文件系统
		conf.set("fs.defaultFS", "hdfs://zhu:9000");
		//2.设置job提交到哪去运行,下面写的是交给yarn运行，如果在本地运行就写local
		conf.set("mapreduce.framework.name", "yarn");
		conf.set("yarn.resourcemanager.hostname", "zhu");
		//3.如果要从windows系统上运行这个job提交客户端程序，则需要加这个跨平台提交的参数
		conf.set("mapreduce.app-submission.cross-platform", "true");
		
		Job job = Job.getInstance();
	
		//1.封装参数：jar包所在的位置
		job.setJar("e:/wc.jar");
		//job.setJarByClass(JobSubmitter.class);//东台获取jar的位置
		
		//2.丰庄参数：本次job所要调用的mapper实现类。reducer实现类
		job.setMapperClass(WordcountMapper.class);
		job.setReducerClass(WordcountReduce.class);
		
		//3.封装参数：本次job的mapper实现类，reducer实现类产生的结果数据的key，value类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		Path output = new Path("hdfs://zhu:9000/wordcount/output");
		FileSystem fs = FileSystem.get(new URI("hdfs://zhu:9000"),conf,"root");
		if (fs.exists(output)){
			fs.delete(output,true);
		}
		//4.封装参数：本次job要处理的输入数据集所在路径，最终输出路径
		FileInputFormat.setInputPaths(job,new Path("hdfs://zhu:9000/wordcount/word"));
		FileOutputFormat.setOutputPath(job, output);//注意：输出路径必须不存在
		
		//5.封装参数：想要启动的reduce task的数量
		job.setNumReduceTasks(2);
		// 6、提交job给yarn
		//job.submit()这个命令是客户端将jar包提交给yarn后，yarn就和客户端失去联系，mapredu跑完跑不完客户端都不知道，
		//而下面的那个命令yarn会与resource manager保持联系，resource manager会向客户端反馈，客户端就可以把反馈的信息打到控制台
		//verbose=true就是显示信息
		boolean res = job.waitForCompletion(true);
		//如果mapreduce成功以码0退出，否则返回码-1，这不是必须的，跑脚本时运行的
		System.exit(res?0:-1);
	}

}
