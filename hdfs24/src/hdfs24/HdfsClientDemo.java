package hdfs24;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
//构造hdfs客户端
import org.junit.Before;
import org.junit.Test;

import com.sun.xml.bind.v2.runtime.unmarshaller.XsiNilLoader.Array;
public class HdfsClientDemo {

	public static void main(String[] args) throws Exception {
		/**
		 * Configuration参数对象的机制：
		 *    构造时，会加载jar包中的默认配置 xx-default.xml
		 *    再加载 用户配置xx-site.xml  ，覆盖掉默认参数
		 *    构造完成之后，还可以conf.set("p","v")，会再次覆盖用户配置文件中的参数值
		 */
		// new Configuration()会从项目的classpath中加载core-default.xml hdfs-default.xml core-site.xml hdfs-site.xml等文件
		Configuration conf = new Configuration();
		//指定客户端上传文件到hdfs时需要保存的副本数：2
		conf.set("dfs.replication", "2");
		//指定客户端上传文件到hdfs时切块的规格大小：64M
		conf.set("dfs.blocksize", "64m");
		//上面的配置也可以写在自己建的一个文件hdfs-site.xml
		//构造一个访问指定hdfs系统的客户端对象，参数1：-hdfs系统的uri，参数2：客户端需要指定的参数，参数3：客户端的身份（用户名）
		FileSystem fs = FileSystem.get(new URI("hdfs://zhu:9000"), conf,"root");
		
		//上传一个文件到hdfs中
		fs.copyFromLocalFile(new Path("E:/baiduyun/aaa.txt"), new Path("/"));
		fs.close();
	}
	FileSystem fs = null;
	@Before
	public void init() throws Exception {
		// new Configuration()会从项目的classpath中加载core-default.xml hdfs-default.xml core-site.xml hdfs-site.xml等文件
				Configuration conf = new Configuration();
				//指定客户端上传文件到hdfs时需要保存的副本数：2
				conf.set("dfs.replication", "2");
				//指定客户端上传文件到hdfs时切块的规格大小：64M
				conf.set("dfs.blocksize", "64m");
				//上面的配置也可以写在自己建的一个文件hdfs-site.xml
				//构造一个访问指定hdfs系统的客户端对象，参数1：-hdfs系统的uri，参数2：客户端需要指定的参数，参数3：客户端的身份（用户名）
				fs = FileSystem.get(new URI("hdfs://zhu:9000"), conf,"root");
				
	}
	/*
	 * 从HDFS下载文件到客户端本地磁盘
	 */
	@Test
	public void TestGet() throws IllegalArgumentException, IOException{
		fs.copyToLocalFile(new Path("/aaa.txt"), new Path("f:/"));
		fs.close();
	}
	/*
	 * 从hdfs内部移动文件\修改名称
	 */
	@Test
	public void TestRename() throws IllegalArgumentException, IOException{
		fs.rename(new Path("/install.log"), new Path("/aa.log"));
		fs.close();
	}
	/*
	 * 在hdfs中创建文件夹
	 */
	
	@Test
	public void TestMkdir() throws IllegalArgumentException, IOException{
		fs.mkdirs(new Path("/xx/yy/zz"));
		fs.close();
	}
	
	/*
	 * 在hdfs中删除文件夹或文件
	 */
	
	@Test
	public void TestDel() throws IllegalArgumentException, IOException{
		fs.delete(new Path("/user"),true);
		fs.close();
	}
	/*
	 * 查询hdfs指定目录下的信息
	 */
	
	@Test
	public void TestLs() throws IllegalArgumentException, IOException{
		//返回的是迭代器，每个文件夹有其他文件
		//只查询文件的信息，不显示文件夹的信息
		RemoteIterator<LocatedFileStatus> iter = fs.listFiles(new Path("/"),true);
		while(iter.hasNext()){
			LocatedFileStatus status = iter.next();
			System.out.println("文件全路径"+status.getPath());
			System.out.println("块大小"+status.getBlockSize());
			System.out.println("文件长度"+status.getLen());
			System.out.println("副本数量"+status.getReplication());
			System.out.println("块信息"+Arrays.toString(status.getBlockLocations()));
			
			System.out.println("------------------------");
		}
		fs.close();
	}
	@Test
	public void TestLs1() throws IllegalArgumentException, IOException{
		//返回的是数组，只看指定目录的一级，不会递归
		//只查询文件的信息，不显示文件夹的信息
		FileStatus[] listStatus = fs.listStatus(new Path("/"));
		for(FileStatus status:listStatus){

			System.out.println("文件全路径"+status.getPath());
			System.out.println(status.isDirectory()?"这是文件夹":"这是文件");
			System.out.println("块大小"+status.getBlockSize());
			System.out.println("文件长度"+status.getLen());
			System.out.println("副本数量"+status.getReplication());
			
			System.out.println("------------------------");
		}
		fs.close();
	}
	/*
	 * 读取hdfs中的文件内用
	 */
	@Test
	public void testReadData() throws IllegalArgumentException, IOException{
		FSDataInputStream in = fs.open(new Path("/test.txt"));//数据流
		//带缓冲的字符流
		//只能从头开始读，也可以指定偏移量范围的读取
		BufferedReader br = new BufferedReader(new InputStreamReader(in,"utf-8"));
		//一次读一行
		String line=null;
		while((line=br.readLine())!=null){
			System.out.println(line);
		}
		
//		byte[] buf = new byte[1024];
//		in.read(buf);  //这两句读比较底层，可以用上面的封装的字符流读
//		System.out.println(new String(buf));
		br.close();
		in.close();
		fs.close();
	}
	/*
	 * 读取hdfs中的指定偏移量范围的内用
	 * 作业题：用本例中的知识，实现读取一个文本中指定block块的所有数据
	 */
	@Test
	public void testReadData1() throws IllegalArgumentException, IOException{
		FSDataInputStream in = fs.open(new Path("/xx.dat"));//数据流
		
		//将读取的起始文件进行指定
		in.seek(12);
		
		//读16个字节
		byte[] buf=new byte[16];
		in.read(buf);
		System.out.println(new String(buf));
		
		in.close();
		fs.close();
	}
	/*
	 * 实现读取一个文本中指定block块的所有数据
	 */
	
	/*
	 * 往hdfs中写数据
	 */
	@Test
	public void testWriterData() throws IllegalArgumentException, IOException{
		FSDataOutputStream out = fs.create(new Path("/zz.jpg"), false);//true覆盖，false追加
		//字节流
		FileInputStream in = new FileInputStream("E:/11.jpg");
		byte[] buf=new byte[1024];
		int read=0;
		while((read=in.read(buf))!=-1){
			out.write(buf,0,read);
			//从buf中读取0，read长度的字节，因为每次buf中1024个字节读到文件中，然后下一次读的时候后面的会覆盖前面的，
			//当最后一次读时，不足1024个字节时，eg只有500个覆盖上一次的前五百个字节，后面仍然保留剩余的上次的部分就会出错
		}
		
//		byte[] buf=new byte[1024];
//		out.write(buf);
		
		in.close();
		out.close();
		fs.close();
		
	}
	

}
