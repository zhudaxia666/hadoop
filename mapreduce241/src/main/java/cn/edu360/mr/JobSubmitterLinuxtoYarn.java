package cn.edu360.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * 如果要在hadoop集群的某台机器上启动这个job提交客户端的话
 * conf里面就不需要指定 fs.defaultFS   mapreduce.framework.name
 * 
 * 因为在集群机器上用 hadoop jar xx.jar cn.edu360.mr.wc.JobSubmitter2 命令来启动客户端main方法时，
 *   hadoop jar这个命令会将所在机器上的hadoop安装目录中的jar包和配置文件加入到运行时的classpath中
 *   
 *   那么，我们的客户端main方法中的new Configuration()语句就会加载classpath中的配置文件，自然就有了 
 *   fs.defaultFS 和 mapreduce.framework.name 和 yarn.resourcemanager.hostname 这些参数配置
 *   
 * @author ThinkPad
 *
 */
public class JobSubmitterLinuxtoYarn {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
//		conf.set("fs.defaultFS", "hdfs://zhu:9000");
//		conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
		// 没指定默认文件系统
		// 没指定mapreduce-job提交到哪运行
		Job job = Job.getInstance(conf);
		job.setJarByClass(JobSubmitterLinuxtoYarn.class);
		
		job.setMapperClass(WordcountMapper.class);
		job.setReducerClass(WordcountReduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path("/wordcount/word"));
		FileOutputFormat.setOutputPath(job, new Path("/wordcount/output3"));
		
		job.setNumReduceTasks(3);
		
		boolean res = job.waitForCompletion(true);
		System.exit(res?0:1);
		

	}

}
