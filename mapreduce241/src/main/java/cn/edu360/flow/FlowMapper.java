package cn.edu360.flow;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBeat>{
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBeat>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] fields = line.split("\t");
		
		String phone = fields[1];
		
		int upflow = Integer.parseInt(fields[fields.length-3]);
		int dflow = Integer.parseInt(fields[fields.length-2]);
		
		context.write(new Text(phone), new FlowBeat(phone,upflow,dflow));
			
		}
		
		
	}


