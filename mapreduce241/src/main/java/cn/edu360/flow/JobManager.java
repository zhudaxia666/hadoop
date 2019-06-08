package cn.edu360.flow;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JobManager {

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(JobManager.class);
		
		job.setMapperClass(FlowMapper.class);
		job.setReducerClass(FlowReducer.class);
		
		// ���ò�����maptask�������ݷ���ʱ�����ĸ������߼���  �������ָ����������Ĭ�ϵ�HashPartitioner��
		job.setPartitionerClass(ProvicePartitioner.class);
		// �������ǵ�ProvincePartitioner���ܻ����6�ַ����ţ����ԣ���Ҫ��6��reduce task������
		job.setNumReduceTasks(6);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBeat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBeat.class);
		
		FileInputFormat.setInputPaths(job, new Path("E:\\hadoop_data\\input"));
		FileOutputFormat.setOutputPath(job, new Path("E:\\hadoop_data\\parttitioner_put"));
		
//		job.setNumReduceTasks(2);
		
		
		job.waitForCompletion(true);

	}

}
