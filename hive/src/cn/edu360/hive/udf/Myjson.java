package cn.edu360.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

public class Myjson extends UDF{
	// ���ظ����е�һ������evaluate()
	public String evaluate(String json,int index) {
		// {"movie":"1193","rate":"5","timeStamp":"978300760","uid":"1"}
		String[] fields = json.split("\"");
		String movie = fields[3];  //1
		String rate = fields[7];   //2
		String ts = fields[11];   //3
		String uid = fields[15];   //4
		
		
		return fields[4*index-1];

	}

}

