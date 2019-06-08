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
 * ���Ҫ��hadoop��Ⱥ��ĳ̨�������������job�ύ�ͻ��˵Ļ�
 * conf����Ͳ���Ҫָ�� fs.defaultFS   mapreduce.framework.name
 * 
 * ��Ϊ�ڼ�Ⱥ�������� hadoop jar xx.jar cn.edu360.mr.wc.JobSubmitter2 �����������ͻ���main����ʱ��
 *   hadoop jar�������Ὣ���ڻ����ϵ�hadoop��װĿ¼�е�jar���������ļ����뵽����ʱ��classpath��
 *   
 *   ��ô�����ǵĿͻ���main�����е�new Configuration()���ͻ����classpath�е������ļ�����Ȼ������ 
 *   fs.defaultFS �� mapreduce.framework.name �� yarn.resourcemanager.hostname ��Щ��������
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
		// ûָ��Ĭ���ļ�ϵͳ
		// ûָ��mapreduce-job�ύ��������
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
