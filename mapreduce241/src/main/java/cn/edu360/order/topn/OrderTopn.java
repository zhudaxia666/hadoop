package cn.edu360.order.topn;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

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
import org.apache.log4j.chainsaw.Main;


public class OrderTopn {
	public static class OrderTopnMapper extends Mapper<LongWritable, Text, Text, OrderBean>{
		OrderBean orderBean = new OrderBean();
		Text k = new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, OrderBean>.Context context)
				throws IOException, InterruptedException {
			String[] fields = value.toString().split(",");
			orderBean.set(fields[0], fields[1], fields[2], Float.parseFloat(fields[3]),Integer.parseInt(fields[4]));
			k.set(fields[0]);
			// �����ｻ��maptask��kv���󣬻ᱻmaptask���л���洢�����Բ��õ��ĸ��ǵ�����
			context.write(k, orderBean);
			
		}
	}
	public static class OrderTopnReducer extends Reducer<Text, OrderBean, OrderBean, NullWritable>{
		
		@Override
		protected void reduce(Text key, Iterable<OrderBean> values,
				Reducer<Text, OrderBean, OrderBean, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// ��ȡtopn�Ĳ���
			int topn = context.getConfiguration().getInt("order.top.n",3);
			ArrayList<OrderBean> beanlist=new ArrayList<OrderBean>();
			
			for (OrderBean orderBean : values) {
				//����һ���µĶ������洢���ε���������ֵ
				OrderBean newBean = new OrderBean();
				newBean.set(orderBean.getOrderId(), orderBean.getUserId(), orderBean.getPdtName(), orderBean.getPrice(), orderBean.getNumber());
				beanlist.add(newBean);
			}
			// ��beanList�е�orderBean�������򣨰��ܽ���С��������,����ܽ����ͬ�������Ʒ���ƣ�
			Collections.sort(beanlist);
			for (int i=0;i<topn;i++){
				context.write(beanlist.get(i), NullWritable.get());
			}
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.setInt("order.top.n",2);
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(OrderTopn.class);
		
		job.setMapperClass(OrderTopnMapper.class);
		job.setReducerClass(OrderTopnReducer.class);
		
		job.setNumReduceTasks(2);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(OrderBean.class);
		job.setOutputKeyClass(OrderBean.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path("E:\\hadoop_data\\order\\input"));
		FileOutputFormat.setOutputPath(job, new Path("E:\\hadoop_data\\order\\output"));
		
//		job.setNumReduceTasks(2);
		
		
		job.waitForCompletion(true);
	}

}
