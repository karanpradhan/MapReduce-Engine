package edu.upenn.cis455.mapreduce.worker;

import java.util.ArrayList;

public class ReducerMinionPool {

	ArrayList<String> buff;
	int no_minions;
	String output_directory;
	ArrayList<ReducerMinion> list_minions;
	String job;
	public ReducerMinionPool(ArrayList<String> buff,int no_minions,String output_directory,String job)
	{
		int j=1;
		list_minions = new ArrayList<ReducerMinion>();
		for(int i=0;i<no_minions;i++)
		{
			ReducerMinion m = new ReducerMinion(job,output_directory,buff,j);
			list_minions.add(m);
			j++;
		}
	}
	public void sync_all()
	{
		for(ReducerMinion m:list_minions)
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
