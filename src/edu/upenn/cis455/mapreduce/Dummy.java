package edu.upenn.cis455.mapreduce;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;

public class Dummy implements Context,Job{

	HashMap<String,ArrayList<String>> map1;
	Dummy()
	{
		map1 =new HashMap<String,ArrayList<String>>();
	}
	public void map(String key,String value,Context c)
	{
		write(key, "1");
	}
	public void reduce(String key,String[] values,Context c)
	{
		for(int i=0;i<values.length;i++)
		{
			
		}
	}
	public void write(String key,String value)
	{
		if(map1.containsKey(key))
		{
			ArrayList<String> temp = map1.get(key);
			temp.add(value);
			map1.put(key,temp);
		}
		else
		{
			ArrayList<String> temp = new ArrayList<String>();
			temp.add(value);
			map1.put(key, temp);
		}
	}
	public static void main(String arg[])
	{
		BigInteger b = new BigInteger("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF",16);
		System.out.println("All Fs: " + b);
		BigInteger bin = b.divide(new BigInteger("2"));
		ArrayList<BigInteger> x =new ArrayList<BigInteger>();
		BigInteger prev = new BigInteger("0");
		for(int i=0;i<1;i++)
		{
			
			prev=prev.add(bin);
			x.add(prev);
			
		}
		x.add(b);
		System.out.println(x);
		
		String key = "a328ddc0e26fba14d801e32aab1cb484deb56193";
		
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
	     System.out.println("Hash value "+hash_value);
	     BigInteger ip = new BigInteger(hash_value,16);
	     System.out.println("bigint is "+ip);
		
		for(BigInteger cmp : x )
		{
			if(cmp.compareTo(ip) == 1 || cmp.compareTo(ip) == 0)
			{
				System.out.println("the range is "+cmp);
				break;
			}
		}
		
	}
}
