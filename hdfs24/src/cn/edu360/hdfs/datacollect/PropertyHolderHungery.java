package cn.edu360.hdfs.datacollect;

import java.util.Properties;

/*
 * �������ģʽ����ʽ1������ʽ����
 */

public class PropertyHolderHungery {
	private static Properties prop = new Properties();
	static{
		try{
			//load�����Ǹ��ļ������������и�class��������class���������������ʱ����Ȼ��Ȼ֪����������ģ�֪��
			prop.load(PropertyHolderHungery.class.getClassLoader().getResourceAsStream("collect.properties"));
			
		}catch (Exception e) {
//			e.printStackTrace(s);
		}
	}
	public static Properties getProps() throws Exception{
		return prop;
	}

}
