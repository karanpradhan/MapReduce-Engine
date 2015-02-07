package edu.upenn.cis455.mapreduce.job;

import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.Job;

public class WordCount {

  public void map(String key, String value, Context context)
  {
    // Your map function for WordCount goes here
	  System.out.println("the value is " + value);
	 // String[] arr = value.split("\t");
     // doc.set(arr[0]);
     String[] values= value.split(",");
     StringBuilder sb= new StringBuilder();
     for(int i=1; i<values.length; i++)
     {
      double d= Double.parseDouble(values[0]);
      double d1=values.length*1.0;
      double ans= d/(d1-1);
  //    Text t = new Text(ans+"");
//    System.out.println(values[i]+".,. "+ans);
//sLogger.debug("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&"+ans); 
      context.write(values[i],ans+"");
      sb.append(values[i]+",");
    }
    String a = sb.toString().substring(0,sb.toString().length()-1);
    //Text b= new Text(a);
      context.write(key, a);
    //  System.out.println(arr[0]+",."+a);
  }
  
  public void reduce(String key, String[] values, Context context)
  {
    // Your reduce function for WordCount goes here
	 //context.write(key, values.length+"");
	  
	  boolean first = true;
      double sum=0.0;
      String list="";
        String now="";
      for  (int i =0 ;i<values.length;i++) {
          now = values[i];
          if(isNumeric(now))
            sum = sum + 0.85*Double.parseDouble(now);
          else
            list=now;
      }

      context.write(key,new String(sum+","+list));
	  
	  
  }
  
  
  public static boolean isNumeric(String str)
{
  return str.matches("-?\\d+(\\.\\d+)?");  //match a number with optional '-' and decimal.
}

  
}
