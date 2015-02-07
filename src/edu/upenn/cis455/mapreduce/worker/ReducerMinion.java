package edu.upenn.cis455.mapreduce.worker;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.job.WordCount;

public class ReducerMinion implements Runnable,Context{
	String job;
	Thread t;
	static ArrayList<String> buff;
	String output_directory;
	//Class myJob;
	ConcurrentHashMap<String,ArrayList<String>> temp;
	//Object reducer;
	//WordCount word_count;
	Object lock = new Object();
	FileWriter fw;
	ReducerMinion(String job,String output_directory,ArrayList<String> buff,int i)
	{
		temp = new ConcurrentHashMap<String,ArrayList<String>>();
		this.job=job;
		this.output_directory = output_directory;
		this.buff=buff;
		t=new Thread(this,""+i);
		t.start();

	}
	public void run()
	{
		Class myJob = null;

		try {
			myJob = Class.forName(job);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Class params[] = new Class[3];
		params[0] = String.class;
		params[1] = String[].class;
		params[2] = Context.class;
		Object word_count = null;
		try {
			word_count = myJob.newInstance();
		} catch (InstantiationException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		} catch (IllegalAccessException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		Method method=null;
		try {
			method = myJob.getDeclaredMethod("reduce", params);
		} catch (SecurityException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		} catch (NoSuchMethodException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		
		
		
		
		
		while(!buff.isEmpty()){
			
			Object line_fwd[] = read();
			if(line_fwd==null){
				break;
			}
			else
			{
			String key = (String)line_fwd[0];
			ArrayList<String> s = (ArrayList<String>) line_fwd[1];
			String res[] = new String [s.size()];
			res = s.toArray(res);
			
			try {
				method.invoke(word_count,key,res,this);
			} catch (IllegalArgumentException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (IllegalAccessException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (InvocationTargetException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
			
			
			//word_count.reduce(key,res,this);
			}
			/*
			String key = line_fwd[0].split("\t")[0];
			String value = line_fwd[0].split("\t")[1];
			if(temp.containsKey(key))
			{
				ArrayList<String> x=temp.get(key);
				x.add(value);
				temp.put(key,x);
			}
			else
			{
				ArrayList<String> v = new ArrayList<String>();
				v.add(value);
				temp.put(key,v);
			}
			if(line_fwd[0].equals(line_fwd[1]))
			{
				continue;
			}
			else
			{
				String res[] = new String[temp.get(key).size()];
				res = temp.get(key).toArray(res);
				word_count.reduce(key,res,this);	
				temp.remove(key);
			}
			*/
			System.out.println(t.toString()+" reduced");

			try {
				t.sleep(0000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		//System.out.println("die");
	
	}
	public void write(String key,String value)
	{
		WorkerServlet.in_wr();
		String f = output_directory+"/a.out";
		File fx = new File(f);
		fw = null;
		try {
			fw = new FileWriter(f,true);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		synchronized(lock)
		{


			try {
				fw.write(key+"\t"+value+"\n");
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
				fw.close();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}
	static synchronized Object[] read()
	{
			if(buff.isEmpty()){
				
				return null;
			}
			else{
			ArrayList<String> temp = new ArrayList<String>();
			String first[] = buff.remove(0).split("\t");
			String key = first[0];
			temp.add(first[1]);
			WorkerServlet.keysRead++;
			while(!buff.isEmpty()){
				if(!buff.isEmpty())
				{
				String next = buff.get(0);
				//System.out.println(t.toString()+" "+next);
				if(next.split("\t")[0].equals(key))
				{
					temp.add(next.split("\t")[1]);
					buff.remove(0);
					WorkerServlet.inc_read();
					
				}
				else
					break;
				}
				else
					return null;
			}
			
			
			Object ret[] = new Object[2];
			ret[0]=key;
			ret[1]=temp;
			
			return ret;
			}
		}
	}

	

	



