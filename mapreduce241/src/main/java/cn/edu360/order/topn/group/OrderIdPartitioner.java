package cn.edu360.order.topn.group;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class OrderIdPartitioner extends Partitioner<OrderBean, NullWritable>{

	@Override
	public int getPartition(OrderBean key, NullWritable value, int numPartitions) {
		// ���ն����е�orderid���ַ�����
		return (key.getOrderId().hashCode() & Integer.MAX_VALUE) % numPartitions;//&Integer.max��ֹ���ָ���
	}
	

}
