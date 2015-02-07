package edu.upenn.cis455.mapreduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import edu.upenn.cis455.mapreduce.worker.ReducerMinionPool;

public class Dummy2 {

	public static void main(String arg[]) throws IOException
	{
		//String cmd = "sort -k1 "
		String job = "edu.upenn.cis455.mapreduce.job.WordCount";
		String op_dir = "/Users/karanpr/Documents/workspace/IWS/worker1";
		String numThreads = "10";
		String directory = "/Users/karanpr/Documents/workspace/IWS/worker1/spool-in";
		
		
		
		File[] file_list = new File(directory).listFiles();
		String files = "";
		for(int i=0;i<file_list.length;i++)
		{
			if(!file_list[i].getName().startsWith(".")){
			try {
				files = files + " " + file_list[i].getCanonicalPath() + " ";
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			}
		}
		files = files.trim();
		String cmd = "sort -k1 "+files;
		Process p = Runtime.getRuntime().exec(cmd);
		BufferedReader br;
		br = new BufferedReader(new InputStreamReader(p.getInputStream()));
		String line = "";
		ArrayList<String> lines = new ArrayList<String>();
		while((line=br.readLine())!=null)
		{
			lines.add(line);
		}
		br.close();
		System.out.println(lines);
		ReducerMinionPool rpool = new ReducerMinionPool(lines,Integer.parseInt(numThreads),op_dir, job);
		rpool.sync_all();
		System.out.println("Done reducing!");
	}
	
	static void delete(File f)
	{
		if(f.isFile())
		{
			f.delete();
		}
		else
		{
			File[] list = f.listFiles();
			if (list.length == 0)
				f.delete();
			for(int i=0;i<list.length;i++)
			{
				delete(list[i]);
			}
			if(f.exists()){
			if(f.list().length == 0)
				f.delete();
			}
		}
	}
	
}
