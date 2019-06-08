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
		
		// �ڴ���������JVMϵͳ���������ڸ�job��������ȡ����HDFS���û���ݡ�
		//Ҳ������run configuration�����ò���-DHADOOP_USER_NAME=root
		System.setProperty("HADOOP_USER_NAME", "root");
		
		Configuration conf = new Configuration();
		//1.����job����ʱҪ���ʵ�Ĭ���ļ�ϵͳ
		conf.set("fs.defaultFS", "hdfs://zhu:9000");
		//2.����job�ύ����ȥ����,����д���ǽ���yarn���У�����ڱ������о�дlocal
		conf.set("mapreduce.framework.name", "yarn");
		conf.set("yarn.resourcemanager.hostname", "zhu");
		//3.���Ҫ��windowsϵͳ���������job�ύ�ͻ��˳�������Ҫ�������ƽ̨�ύ�Ĳ���
		conf.set("mapreduce.app-submission.cross-platform", "true");
		
		Job job = Job.getInstance();
	
		//1.��װ������jar�����ڵ�λ��
		job.setJar("e:/wc.jar");
		//job.setJarByClass(JobSubmitter.class);//��̨��ȡjar��λ��
		
		//2.��ׯ����������job��Ҫ���õ�mapperʵ���ࡣreducerʵ����
		job.setMapperClass(WordcountMapper.class);
		job.setReducerClass(WordcountReduce.class);
		
		//3.��װ����������job��mapperʵ���࣬reducerʵ��������Ľ�����ݵ�key��value����
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		Path output = new Path("hdfs://zhu:9000/wordcount/output");
		FileSystem fs = FileSystem.get(new URI("hdfs://zhu:9000"),conf,"root");
		if (fs.exists(output)){
			fs.delete(output,true);
		}
		//4.��װ����������jobҪ������������ݼ�����·�����������·��
		FileInputFormat.setInputPaths(job,new Path("hdfs://zhu:9000/wordcount/word"));
		FileOutputFormat.setOutputPath(job, output);//ע�⣺���·�����벻����
		
		//5.��װ��������Ҫ������reduce task������
		job.setNumReduceTasks(2);
		// 6���ύjob��yarn
		//job.submit()��������ǿͻ��˽�jar���ύ��yarn��yarn�ͺͿͻ���ʧȥ��ϵ��mapredu�����ܲ���ͻ��˶���֪����
		//��������Ǹ�����yarn����resource manager������ϵ��resource manager����ͻ��˷������ͻ��˾Ϳ��԰ѷ�������Ϣ�򵽿���̨
		//verbose=true������ʾ��Ϣ
		boolean res = job.waitForCompletion(true);
		//���mapreduce�ɹ�����0�˳������򷵻���-1���ⲻ�Ǳ���ģ��ܽű�ʱ���е�
		System.exit(res?0:-1);
	}

}
