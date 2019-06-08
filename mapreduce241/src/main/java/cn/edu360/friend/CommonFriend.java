package cn.edu360.friend;

import java.io.IOException;
import java.util.ArrayList;
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

public class CommonFriend {
	public static class CommonFriendMapper extends Mapper<LongWritable, Text, Text, Text>{
		//A:B,C,D,F,E,O
		// 输出：　B->A  C->A  D->A ...
		Text k=new Text();
		Text v=new Text();
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			String[] fields = value.toString().split(":");
			String user = fields[0];
			String[] friends = fields[1].split(",");
			v.set(user);
			for (String f : friends) {
				k.set(f);
				
				context.write(k, v);
			}
			
			
		}
	}
	public static class CommonFriendReducer extends Reducer<Text, Text, Text, Text>{
		// 一组数据：  B -->  A  E  F  J .....
		// 一组数据：  C -->  B  F  E  J .....
		@Override
		protected void reduce(Text friend, Iterable<Text> users, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			ArrayList<String> userlist = new ArrayList<String>();
			for (Text user : users) {
				userlist.add(user.toString());
			}
			Collections.sort(userlist);
			
			for(int i=0;i<userlist.size()-1;i++){
				for (int j=i+1;j<userlist.size();j++){
					context.write(new Text(userlist.get(i)+"-"+userlist.get(j)),friend);
				}
			}
			
			
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
//		conf.setInt("order.top.n",2);
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(CommonFriend.class);
		
		job.setMapperClass(CommonFriendMapper.class);
		job.setReducerClass(CommonFriendReducer.class);
		
		job.setNumReduceTasks(2);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path("E:\\hadoop_data\\friends\\input"));
		FileOutputFormat.setOutputPath(job, new Path("E:\\hadoop_data\\friends\\output"));
		
//		job.setNumReduceTasks(2);
		
		
		job.waitForCompletion(true);
	}

}
