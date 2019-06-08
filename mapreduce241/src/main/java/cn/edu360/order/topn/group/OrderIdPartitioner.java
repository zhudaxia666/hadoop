package cn.edu360.order.topn.group;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class OrderIdPartitioner extends Partitioner<OrderBean, NullWritable>{

	@Override
	public int getPartition(OrderBean key, NullWritable value, int numPartitions) {
		// 按照订单中的orderid来分发数据
		return (key.getOrderId().hashCode() & Integer.MAX_VALUE) % numPartitions;//&Integer.max防止出现负数
	}
	

}
