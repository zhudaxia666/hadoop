package cn.edu360.hdfs.wordcount;



public interface Mapper {
	public void map(String line,Context context);//context是用来装东西的,装line

}
