package edu.upenn.cis455.mapreduce.worker;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;

public class MinionPool {

	ArrayList<String> buff;
	ArrayList<String> workers;
	LinkedHashMap<String,BigInteger> ranges;
	String job;
	int no_minions;
	ArrayList<Minion> list_minions;
	String working_directory;
	
	MinionPool(ArrayList<String> buff,int no_minions,String working_directory,String job,ArrayList<String> workers,LinkedHashMap<String,BigInteger> ranges)
	{
		int j=1;
		this.buff = buff;
		this.no_minions = no_minions;
		list_minions = new ArrayList<Minion>();
		this.working_directory = working_directory;
		this.job = job;
		this.workers = workers;
		for(int i=0;i<no_minions;i++)
		{
			Minion m = new Minion(buff,working_directory,j,job,workers,ranges);
			list_minions.add(m);
			j++;
		}
	}
	
	void sync_all()
	{
		for(Minion m : list_minions)
		{
			try {
				m.t.join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
