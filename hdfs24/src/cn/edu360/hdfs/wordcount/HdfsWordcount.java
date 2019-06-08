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
		 * 初始化工作
		 */
		Properties props = new Properties();
		props.load(HdfsWordcount.class.getClassLoader().getResourceAsStream("job.properties"));
//		Class.forName(xxx.xx.xx)返回的是一个类。
//		Class.forName(xxx.xx.xx)的作用是要求JVM查找并加载指定的类，也就是说JVM会执行该类的静态代码段
		Class<?> mapper_class = Class.forName(props.getProperty("MAPPER_CLASS"));
		Mapper mapper =(Mapper) mapper_class.newInstance();
		
		Context context = new Context();
		Path input = new Path(props.getProperty("INPUT_PATH"));
		Path output = new Path(props.getProperty("OUTPUT_PATH"));
		/*
		 * 处理数据
		 */
		//连接hdfs
		FileSystem fs = FileSystem.get(new URI("hdfs://zhu:9000"), new Configuration(), "root");
		RemoteIterator<LocatedFileStatus> iter = fs.listFiles(input,false);//false表示不递归
		
		
		
		while(iter.hasNext()){
			LocatedFileStatus file = iter.next();
			//读数据
			FSDataInputStream in = fs.open(file.getPath());
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String line=null;
			//逐行读取
			while((line=br.readLine())!=null){
				//调用一个方法对每一行进行业务处理
				//之所以使用面向接口编程，就是为了以后方便换实现（换逻辑），为了以后换新的实现类新的实现接口，使用new 实现类会把接口写死，所以使用反射配置文件
				mapper.map(line, context);
			}
			br.close();
			in.close();	
		}
		
		/*
		 * 输出数据
		 */
		HashMap<Object, Object> contextMap = context.getContextMap();
		if (fs.exists(output)){
			throw new RuntimeException("指定的输出目录已存在，请更换。。。！");
		}
		FSDataOutputStream out = fs.create(new Path(output, new Path("res.dat")));//父子路径
		//取出hashmap中的数据
		Set<Entry<Object, Object>> entrySet = contextMap.entrySet();
		for(Entry<Object,Object> entry:entrySet){
			out.write((entry.getKey().toString()+"\t"+entry.getValue()+"\n").getBytes());
		}
		out.close();
		fs.close();
		System.out.println("恭喜！数据统计完成。。。。");

	}

}
