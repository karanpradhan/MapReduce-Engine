package edu.upenn.cis455.mapreduce.worker;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;

import javax.servlet.http.HttpServlet;

import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.Job;
import edu.upenn.cis455.mapreduce.job.WordCount;

public class Minion implements Runnable,Context{

	LinkedHashMap<String,BigInteger> ranges;
	ArrayList<String> workers;
	String job;
	Thread t;
	static ArrayList<String> buff;
	
	String working_directory;
	Minion(ArrayList<String> buff,String working_directory,int i,String job,ArrayList<String> workers,LinkedHashMap<String,BigInteger> ranges)
	{
		this.job=job;
		this.working_directory= working_directory;
		this.buff = buff;
		t=new Thread(this,"Minion"+i);
		this.ranges = ranges;
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
		params[1] = String.class;
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
			method = myJob.getDeclaredMethod("map", params);
		} catch (SecurityException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		} catch (NoSuchMethodException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		
			while(!buff.isEmpty())
			{
				/*System.out.println(t.toString()+"..."+remove());
				try {
					t.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}*/
				String line = remove();
				//System.out.println(line);
				
				if(line==null)
				{
					break;
				}
				else
				{
				String temp[]=  line.split("\t");
				String key = temp[0];
				String value = temp[1];
				
				try {
					method.invoke(word_count,key,value,this);
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
				try {
					t.sleep(0000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				//write(key,value);
			//	System.out.println(t.toString());
				}
			}
			
		//System.out.println(t.toString()+">>>"+"run complete");
			
	}
	static synchronized String remove()
	{
		
		if(!buff.isEmpty())
		{
			WorkerServlet.inc_read();
			return buff.remove(0);
		}
		else
		{
			return null;
			
		}
		
	}
	public void write(String key,String value)
	{
		WorkerServlet.in_wr();
		MessageDigest mDigest=null;
		try {
			mDigest = MessageDigest.getInstance("SHA1");
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		byte[] result = mDigest.digest(key.getBytes());
		 StringBuffer sb = new StringBuffer();
	        for (int i = 0; i < result.length; i++) {
	            sb.append(Integer.toString((result[i] & 0xff) + 0x100, 16).substring(1));
	        }
	         
	     String hash_value = sb.toString();
	     
	     BigInteger hash = new BigInteger(hash_value,16);
	     String chosen_worker = "";
	     
	     for(String wkr : ranges.keySet())
			{
				if(ranges.get(wkr).compareTo(hash) == 1 || ranges.get(wkr).compareTo(hash) == 0)
				{
					chosen_worker = wkr;
					break;
				}
			}
	     
	     String f = working_directory+"/spool-out"+"/"+chosen_worker+".out";
	     File fx = new File(f);
	     FileWriter fw = null;
	     try {
			fw = new FileWriter(f,true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    synchronized(fw)
	    {
	    	try {
				fw.write(key+"\t"+value+"\n");
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    }
	    try {
			fw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 

	}
	
}
