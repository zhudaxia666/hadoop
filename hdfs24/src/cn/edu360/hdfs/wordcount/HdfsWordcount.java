package cn.edu360.hdfs.wordcount;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
//import org.mortbay.jetty.servlet.Context;

public class HdfsWordcount {

	public static void main(String[] args) throws Exception{
		/*
		 * ��ʼ������
		 */
		Properties props = new Properties();
		props.load(HdfsWordcount.class.getClassLoader().getResourceAsStream("job.properties"));
//		Class.forName(xxx.xx.xx)���ص���һ���ࡣ
//		Class.forName(xxx.xx.xx)��������Ҫ��JVM���Ҳ�����ָ�����࣬Ҳ����˵JVM��ִ�и���ľ�̬�����
		Class<?> mapper_class = Class.forName(props.getProperty("MAPPER_CLASS"));
		Mapper mapper =(Mapper) mapper_class.newInstance();
		
		Context context = new Context();
		Path input = new Path(props.getProperty("INPUT_PATH"));
		Path output = new Path(props.getProperty("OUTPUT_PATH"));
		/*
		 * ��������
		 */
		//����hdfs
		FileSystem fs = FileSystem.get(new URI("hdfs://zhu:9000"), new Configuration(), "root");
		RemoteIterator<LocatedFileStatus> iter = fs.listFiles(input,false);//false��ʾ���ݹ�
		
		
		
		while(iter.hasNext()){
			LocatedFileStatus file = iter.next();
			//������
			FSDataInputStream in = fs.open(file.getPath());
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String line=null;
			//���ж�ȡ
			while((line=br.readLine())!=null){
				//����һ��������ÿһ�н���ҵ����
				//֮����ʹ������ӿڱ�̣�����Ϊ���Ժ󷽱㻻ʵ�֣����߼�����Ϊ���Ժ��µ�ʵ�����µ�ʵ�ֽӿڣ�ʹ��new ʵ�����ѽӿ�д��������ʹ�÷��������ļ�
				mapper.map(line, context);
			}
			br.close();
			in.close();	
		}
		
		/*
		 * �������
		 */
		HashMap<Object, Object> contextMap = context.getContextMap();
		if (fs.exists(output)){
			throw new RuntimeException("ָ�������Ŀ¼�Ѵ��ڣ��������������");
		}
		FSDataOutputStream out = fs.create(new Path(output, new Path("res.dat")));//����·��
		//ȡ��hashmap�е�����
		Set<Entry<Object, Object>> entrySet = contextMap.entrySet();
		for(Entry<Object,Object> entry:entrySet){
			out.write((entry.getKey().toString()+"\t"+entry.getValue()+"\n").getBytes());
		}
		out.close();
		fs.close();
		System.out.println("��ϲ������ͳ����ɡ�������");

	}

}
