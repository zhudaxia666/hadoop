package cn.edu360.mr.join;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.htrace.fasterxml.jackson.databind.ser.BeanPropertyWriter;
/**
 * 本例是使用最low的方式实现
 * 
 * 还可以利用Partitioner+CompareTo+GroupingComparator 组合拳来高效实现
 * @author ThinkPad
 */
public class ReduceSideJoin {
	public static class ReduceSideMapper extends Mapper<LongWritable, Text, Text, JoinBean>{
		
		String filename=null;
		JoinBean bean = new JoinBean();
		Text k = new Text();

		/**
		 * maptask在做数据处理时，会先调用一次setup() 钓完后才对每一行反复调用map()
		 */
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, JoinBean>.Context context)
				throws IOException, InterruptedException {
			FileSplit inputSplit = (FileSplit) context.getInputSplit();
			filename = inputSplit.getPath().getName();
		}
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, JoinBean>.Context context)
				throws IOException, InterruptedException {
			
			String[] fields = value.toString().split(",");
			if (filename.startsWith("order")){
				bean.set(fields[0], fields[1], "NULL", -1, "NULL", "order");
			}else{
				bean.set("NULL", fields[0], fields[1], Integer.parseInt(fields[2]), fields[3], "user");
			}
			k.set(bean.getUserId());
			context.write(k, bean);
		}
		
	}

	public static class ReduceSideReducer extends Reducer<Text, JoinBean, JoinBean, NullWritable>{
		@Override
		protected void reduce(Text key, Iterable<JoinBean> beans,
				Reducer<Text, JoinBean, JoinBean, NullWritable>.Context context) throws IOException, InterruptedException {
			
			ArrayList<JoinBean> orderList = new ArrayList<JoinBean>();
			JoinBean userbean = null;
			try {
				//区分两类数据
				for (JoinBean bean : beans) {
					if("order".equals(bean.getTableName())){
						JoinBean newBean = new JoinBean();
						BeanUtils.copyProperties(newBean, bean);
						orderList.add(newBean);
					}else {
						userbean = new JoinBean();
						BeanUtils.copyProperties(userbean, bean);
					}
					
				}
				
				//拼接数据并输出
				for (JoinBean bean : orderList) {
					bean.setUserName(userbean.getUserName());
					bean.setUserAge(userbean.getUserAge());
					bean.setUserFriend(userbean.getUserFriend());
					
					context.write(bean, NullWritable.get());
					
				}
				
			} catch (Exception e) {
				e.printStackTrace();
			}
			
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();  
		
		Job job = Job.getInstance(conf);

		job.setJarByClass(ReduceSideJoin.class);

		job.setMapperClass(ReduceSideMapper.class);
		job.setReducerClass(ReduceSideReducer.class);
		
		job.setNumReduceTasks(2);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(JoinBean.class);
		
		job.setOutputKeyClass(JoinBean.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.setInputPaths(job, new Path("E:\\hadoop_data\\join\\input"));
		FileOutputFormat.setOutputPath(job, new Path("E:\\hadoop_data\\join\\out1"));

		job.waitForCompletion(true);
	}


}
