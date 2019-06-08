package cn.edu360.order.topn.group;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




public class OrderTopn {
	public static class OrderTopnMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable>{
		OrderBean orderBean =new OrderBean();
		NullWritable v=NullWritable.get();
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, OrderBean, NullWritable>.Context context)
				throws IOException, InterruptedException {
			String[] fields = value.toString().split(",");
			orderBean.set(fields[0], fields[1], fields[2], Float.parseFloat(fields[3]),Integer.parseInt(fields[4]));
			context.write(orderBean, v);
			
			
		}
	}
	public static class OrderTopnReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable>{

		/**
		 * ��Ȼreduce�����еĲ���keyֻ��һ��������ֻҪ����������һ�Σ�key�е�ֵ�ͻ��
		 */
		@Override
		protected void reduce(OrderBean key, Iterable<NullWritable> values,
				Reducer<OrderBean, NullWritable, OrderBean, NullWritable>.Context context)
				throws IOException, InterruptedException {
			int i=0;
			for (NullWritable v : values) {
				context.write(key, v);
				if(++i==3)return;
			}
			
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
//		conf.setInt("order.top.n",2);
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(OrderTopn.class);
		
		job.setMapperClass(OrderTopnMapper.class);
		job.setReducerClass(OrderTopnReducer.class);
		//�����Լ�д�ķ�partitioner��ͷ�������
		job.setPartitionerClass(OrderIdPartitioner.class);
		job.setGroupingComparatorClass(OrderIdGroupComparator.class);
		
		job.setNumReduceTasks(2);

		job.setMapOutputKeyClass(OrderBean.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(OrderBean.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path("E:\\hadoop_data\\order\\input"));
		FileOutputFormat.setOutputPath(job, new Path("E:\\hadoop_data\\order\\output1"));
		
//		job.setNumReduceTasks(2);
		
		
		job.waitForCompletion(true);
	}
	

}
