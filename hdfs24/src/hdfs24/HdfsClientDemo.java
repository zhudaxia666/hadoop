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
//����hdfs�ͻ���
import org.junit.Before;
import org.junit.Test;

import com.sun.xml.bind.v2.runtime.unmarshaller.XsiNilLoader.Array;
public class HdfsClientDemo {

	public static void main(String[] args) throws Exception {
		/**
		 * Configuration��������Ļ��ƣ�
		 *    ����ʱ�������jar���е�Ĭ������ xx-default.xml
		 *    �ټ��� �û�����xx-site.xml  �����ǵ�Ĭ�ϲ���
		 *    �������֮�󣬻�����conf.set("p","v")�����ٴθ����û������ļ��еĲ���ֵ
		 */
		// new Configuration()�����Ŀ��classpath�м���core-default.xml hdfs-default.xml core-site.xml hdfs-site.xml���ļ�
		Configuration conf = new Configuration();
		//ָ���ͻ����ϴ��ļ���hdfsʱ��Ҫ����ĸ�������2
		conf.set("dfs.replication", "2");
		//ָ���ͻ����ϴ��ļ���hdfsʱ�п�Ĺ���С��64M
		conf.set("dfs.blocksize", "64m");
		//���������Ҳ����д���Լ�����һ���ļ�hdfs-site.xml
		//����һ������ָ��hdfsϵͳ�Ŀͻ��˶��󣬲���1��-hdfsϵͳ��uri������2���ͻ�����Ҫָ���Ĳ���������3���ͻ��˵���ݣ��û�����
		FileSystem fs = FileSystem.get(new URI("hdfs://zhu:9000"), conf,"root");
		
		//�ϴ�һ���ļ���hdfs��
		fs.copyFromLocalFile(new Path("E:/baiduyun/aaa.txt"), new Path("/"));
		fs.close();
	}
	FileSystem fs = null;
	@Before
	public void init() throws Exception {
		// new Configuration()�����Ŀ��classpath�м���core-default.xml hdfs-default.xml core-site.xml hdfs-site.xml���ļ�
				Configuration conf = new Configuration();
				//ָ���ͻ����ϴ��ļ���hdfsʱ��Ҫ����ĸ�������2
				conf.set("dfs.replication", "2");
				//ָ���ͻ����ϴ��ļ���hdfsʱ�п�Ĺ���С��64M
				conf.set("dfs.blocksize", "64m");
				//���������Ҳ����д���Լ�����һ���ļ�hdfs-site.xml
				//����һ������ָ��hdfsϵͳ�Ŀͻ��˶��󣬲���1��-hdfsϵͳ��uri������2���ͻ�����Ҫָ���Ĳ���������3���ͻ��˵���ݣ��û�����
				fs = FileSystem.get(new URI("hdfs://zhu:9000"), conf,"root");
				
	}
	/*
	 * ��HDFS�����ļ����ͻ��˱��ش���
	 */
	@Test
	public void TestGet() throws IllegalArgumentException, IOException{
		fs.copyToLocalFile(new Path("/aaa.txt"), new Path("f:/"));
		fs.close();
	}
	/*
	 * ��hdfs�ڲ��ƶ��ļ�\�޸�����
	 */
	@Test
	public void TestRename() throws IllegalArgumentException, IOException{
		fs.rename(new Path("/install.log"), new Path("/aa.log"));
		fs.close();
	}
	/*
	 * ��hdfs�д����ļ���
	 */
	
	@Test
	public void TestMkdir() throws IllegalArgumentException, IOException{
		fs.mkdirs(new Path("/xx/yy/zz"));
		fs.close();
	}
	
	/*
	 * ��hdfs��ɾ���ļ��л��ļ�
	 */
	
	@Test
	public void TestDel() throws IllegalArgumentException, IOException{
		fs.delete(new Path("/user"),true);
		fs.close();
	}
	/*
	 * ��ѯhdfsָ��Ŀ¼�µ���Ϣ
	 */
	
	@Test
	public void TestLs() throws IllegalArgumentException, IOException{
		//���ص��ǵ�������ÿ���ļ����������ļ�
		//ֻ��ѯ�ļ�����Ϣ������ʾ�ļ��е���Ϣ
		RemoteIterator<LocatedFileStatus> iter = fs.listFiles(new Path("/"),true);
		while(iter.hasNext()){
			LocatedFileStatus status = iter.next();
			System.out.println("�ļ�ȫ·��"+status.getPath());
			System.out.println("���С"+status.getBlockSize());
			System.out.println("�ļ�����"+status.getLen());
			System.out.println("��������"+status.getReplication());
			System.out.println("����Ϣ"+Arrays.toString(status.getBlockLocations()));
			
			System.out.println("------------------------");
		}
		fs.close();
	}
	@Test
	public void TestLs1() throws IllegalArgumentException, IOException{
		//���ص������飬ֻ��ָ��Ŀ¼��һ��������ݹ�
		//ֻ��ѯ�ļ�����Ϣ������ʾ�ļ��е���Ϣ
		FileStatus[] listStatus = fs.listStatus(new Path("/"));
		for(FileStatus status:listStatus){

			System.out.println("�ļ�ȫ·��"+status.getPath());
			System.out.println(status.isDirectory()?"�����ļ���":"�����ļ�");
			System.out.println("���С"+status.getBlockSize());
			System.out.println("�ļ�����"+status.getLen());
			System.out.println("��������"+status.getReplication());
			
			System.out.println("------------------------");
		}
		fs.close();
	}
	/*
	 * ��ȡhdfs�е��ļ�����
	 */
	@Test
	public void testReadData() throws IllegalArgumentException, IOException{
		FSDataInputStream in = fs.open(new Path("/test.txt"));//������
		//��������ַ���
		//ֻ�ܴ�ͷ��ʼ����Ҳ����ָ��ƫ������Χ�Ķ�ȡ
		BufferedReader br = new BufferedReader(new InputStreamReader(in,"utf-8"));
		//һ�ζ�һ��
		String line=null;
		while((line=br.readLine())!=null){
			System.out.println(line);
		}
		
//		byte[] buf = new byte[1024];
//		in.read(buf);  //��������Ƚϵײ㣬����������ķ�װ���ַ�����
//		System.out.println(new String(buf));
		br.close();
		in.close();
		fs.close();
	}
	/*
	 * ��ȡhdfs�е�ָ��ƫ������Χ������
	 * ��ҵ�⣺�ñ����е�֪ʶ��ʵ�ֶ�ȡһ���ı���ָ��block�����������
	 */
	@Test
	public void testReadData1() throws IllegalArgumentException, IOException{
		FSDataInputStream in = fs.open(new Path("/xx.dat"));//������
		
		//����ȡ����ʼ�ļ�����ָ��
		in.seek(12);
		
		//��16���ֽ�
		byte[] buf=new byte[16];
		in.read(buf);
		System.out.println(new String(buf));
		
		in.close();
		fs.close();
	}
	/*
	 * ʵ�ֶ�ȡһ���ı���ָ��block�����������
	 */
	
	/*
	 * ��hdfs��д����
	 */
	@Test
	public void testWriterData() throws IllegalArgumentException, IOException{
		FSDataOutputStream out = fs.create(new Path("/zz.jpg"), false);//true���ǣ�false׷��
		//�ֽ���
		FileInputStream in = new FileInputStream("E:/11.jpg");
		byte[] buf=new byte[1024];
		int read=0;
		while((read=in.read(buf))!=-1){
			out.write(buf,0,read);
			//��buf�ж�ȡ0��read���ȵ��ֽڣ���Ϊÿ��buf��1024���ֽڶ����ļ��У�Ȼ����һ�ζ���ʱ�����ĻḲ��ǰ��ģ�
			//�����һ�ζ�ʱ������1024���ֽ�ʱ��egֻ��500��������һ�ε�ǰ��ٸ��ֽڣ�������Ȼ����ʣ����ϴεĲ��־ͻ����
		}
		
//		byte[] buf=new byte[1024];
//		out.write(buf);
		
		in.close();
		out.close();
		fs.close();
		
	}
	

}
