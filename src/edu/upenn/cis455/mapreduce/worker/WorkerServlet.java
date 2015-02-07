package edu.upenn.cis455.mapreduce.worker;

import java.io.*;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;

import javax.servlet.*;
import javax.servlet.http.*;

//import org.apache.log4j.logger;
//import org.apache.log4j.PropertyConfigurator;


public class WorkerServlet extends HttpServlet implements Runnable{

	static final long serialVersionUID = 455555002;
	InetAddress master_address;
	int master_port;
	int worker_port;
	int count_worker;
	Thread worker;
	String worker_status;
	String directory;
	String job;
	static int keysRead;
	static int keysWritten;
	//static //logger //logger = //logger.get//logger(WorkerServlet.class);
	boolean sync_flag;
	ArrayList<String> sync_buffer;
	
	//ArrayList<Thread> worker_threads;
	public void init(ServletConfig config) throws ServletException
	{
		//this.config = config;
		//PropertyConfigurator.configure("log/log4j.properties");
		super.init(config);
		String master_parameters = getServletConfig().getInitParameter("master");
		directory = getServletConfig().getInitParameter("storagedir");
		try {
			master_address = InetAddress.getByName(master_parameters.split(":")[0]);
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		worker_port = Integer.parseInt(getServletConfig().getInitParameter("port"));
		job = "no";
		keysRead = 0;
		keysWritten = 0;
		sync_flag = false;
		sync_buffer = new ArrayList<String>();
		master_port = Integer.parseInt(master_parameters.split(":")[1]);
		////logger.debug("<<_ NEW RUN ___________________________________________>>");
		worker = new Thread(this);
		worker.start();
	}
	public void run()
	{
		worker_status="idle";
		while(true)
		{
			
			Socket client= null;
			try {
				client = new Socket(master_address,master_port);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				System.out.println("Trying to connect..");
				continue;
				//e.printStackTrace();
			}
			PrintWriter out=null;

			try {
				out = new PrintWriter(client.getOutputStream(),true);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			//out.println("GET /HW3/workerstatus?keysRead=10&keysWritten=20&job="+job+"&status="+worker_status+"&port="+worker_port+" HTTP/1.1\r\nHost: "+master_address.getHostAddress()+"\r\nConnection: close\r\n");
			out.println("GET /workerstatus?keysRead="+keysRead+""+"&keysWritten="+keysWritten+""+"&job="+job+"&status="+worker_status+"&port="+worker_port+" HTTP/1.1\r\nHost: "+master_address.getHostAddress()+"\r\nConnection: close\r\n");
			try {
				worker.sleep(10000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			out.close();
			try {
				client.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println("worker is on!");
		}
	}

	public void doGet(HttpServletRequest request, HttpServletResponse response) 
			throws java.io.IOException
			{
		response.setContentType("text/html");
		PrintWriter out = response.getWriter();
		out.println("<html><head><title>Worker</title></head>");
		out.println("<body>Hi, I am the worker!</body></html>");
			}

	public void doPost(HttpServletRequest request, HttpServletResponse response) 
			throws java.io.IOException
			{
				////logger.debug("Port:"+worker_port+" GOT Post : "+request.getPathInfo());
				String url = request.getPathInfo();
				if(url.equals("/runmap"))
				{
					keysRead=0;
					keysWritten=0;
					worker_status = "mapping";
					String job = request.getParameter("job");
					String ip_dir = request.getParameter("input");
					String numThreads = request.getParameter("numThreads");
					String numWorkers = request.getParameter("numWorkers");
					this.job = job;
					int len_workers = Integer.parseInt(numWorkers);
					count_worker = len_workers;
					ArrayList<String> workers = new ArrayList<String>();
					File spool_in = new File(directory+"/spool-in");
					if(spool_in.exists())
					{
						delete(spool_in);
					}
					spool_in.mkdirs();
					
					File spool_out = new File(directory+"/spool-out");
					if(spool_out.exists())
					{
						delete(spool_out);
					}
					spool_out.mkdirs();
					//worker_status = "mapping";
					for(int i=0;i<len_workers;i++)
					{
						workers.add(request.getParameter("worker"+(i+1)));
					}
					
					BigInteger b = new BigInteger("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF",16);
					BigInteger no_of_workers = new BigInteger(""+workers.size());
					LinkedHashMap<String,BigInteger> ranges = new LinkedHashMap<String,BigInteger>();
					BigInteger bin = b.divide(no_of_workers);
					BigInteger prev = new BigInteger("0");
					Iterator it_worker = workers.iterator();
					for(int i=0;i<workers.size()-1;i++)
					{
						prev = prev.add(bin);
						ranges.put((String) it_worker.next(), prev);
					}
					ranges.put((String)it_worker.next(),b);
					
					String input_path = directory + ip_dir;
					File[] file_list = new File(input_path).listFiles();
					for(int i=0;i<file_list.length;i++)
					{
						if(!file_list[i].getName().startsWith(".")){
							BufferedReader br = new BufferedReader(new FileReader(file_list[i]));
							ArrayList<String> buff = new ArrayList<String>();
							String line="";
							while((line=br.readLine())!=null)
							{
								buff.add(line);
							}
							@SuppressWarnings("unused")
							MinionPool m_pool = new MinionPool(buff,Integer.parseInt(numThreads),directory,job,workers,ranges);
							m_pool.sync_all();
						}	
					}
					File[] spool_out_list = new File(directory+"/spool-out").listFiles();
					for(int i=0;i<spool_out_list.length;i++)
					{
						String temp[] = spool_out_list[i].getName().split(":");
						String ip_out = temp[0];
						String port_out = temp[1].substring(0,temp[1].indexOf("."));
						Socket client_out = new Socket(InetAddress.getByName(ip_out),Integer.parseInt(port_out));
						PrintWriter pw_out = new PrintWriter(client_out.getOutputStream(),true);
						pw_out.print("POST /pushdata HTTP/1.1\r\n");
						pw_out.print("Host: "+ip_out+"\r\n");
						pw_out.print("Content-Length: "+spool_out_list[i].length()+"\r\n");
						pw_out.print("Content-Type: text/plain\r\n");
						pw_out.print("Connection: close\r\n\r\n");
					 	BufferedReader br = new BufferedReader(new FileReader(spool_out_list[i]));
					 	String line="";
						while((line=br.readLine())!=null)
						{
							pw_out.println(line);
						}
						pw_out.close();
						client_out.close();
						
					}
					
					
					
					
				}
				if(url.equals("/pushdata"))
				{
					
						BufferedReader br = request.getReader();
						String line = "";
						StringBuffer bw = new StringBuffer();
						while((line=br.readLine())!=null)
						{
							bw.append(line+"\n");
						}
						sync_buffer.add(bw.toString());
						sync_flag = true;
						////logger.debug("Port:"+worker_port+" Writing into the sync_buffer");
					
						
						
						if(count_worker==sync_buffer.size()){
							
							for(String i:sync_buffer)
							{
								File f= new File(directory+"/spool-in"+"/"+(serialVersionUID + Math.random()+"").hashCode()+"");
								BufferedWriter bwx = new BufferedWriter(new FileWriter(f));
								bwx.write(i);
								bwx.close();
							}
							////logger.debug("Port:"+worker_port+" spooled in sync buffer");

							worker_status = "waiting";


							Socket client= null;
							try {
								client = new Socket(master_address,master_port);
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							PrintWriter out=null;

							try {
								out = new PrintWriter(client.getOutputStream(),true);
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							out.println("GET /workerstatus?keysRead="+keysRead+""+"&keysWritten="+keysWritten+""+"&job="+job+"&status="+worker_status+"&port="+worker_port+" HTTP/1.1\r\nHost: "+master_address.getHostAddress()+"\r\nConnection: close\r\n");

							//out.println("GET /HW3/workerstatus?keysRead=10&keysWritten=20&job="+job+"&status="+worker_status+"&port="+worker_port+" HTTP/1.1\r\nHost: "+master_address.getHostAddress()+"\r\nConnection: close\r\n");
							out.close();
							client.close();

							sync_buffer.clear();
							//logger.debug("Port:"+worker_port+" done spooling in data");

						}
				}
				if(url.equals("/runreduce"))
				{
					//logger.debug("Port:"+worker_port+" startin reduce");
					keysRead=0;
					keysWritten=0;
					worker_status = "reducing";
					String op_dir = request.getParameter("output");
					String numThreads = request.getParameter("numThreads");
					
					File output_dir = new File(directory+op_dir);
					if(output_dir.exists())
					{
						delete(output_dir);
					}
					output_dir.mkdirs();
					
					
					
					
					
					//System.out.println("Got a reduce!");
					//logger.debug("Port:"+worker_port+" Got reduce!");
					File[] file_list = new File(directory+"/spool-in").listFiles();
					String files = "";
					for(int i=0;i<file_list.length;i++)
					{
						files = files + " " + file_list[i].getCanonicalPath() + " ";
					}
					files = files.trim();
					String cmd = "sort -k1,1 -k2,2 "+files;
					Process p = Runtime.getRuntime().exec(cmd);
					try {
						p.waitFor();
					} catch (InterruptedException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
					BufferedReader br;
					br = new BufferedReader(new InputStreamReader(p.getInputStream()));
					String line = "";
					ArrayList<String> lines = new ArrayList<String>();
					while((line=br.readLine())!=null)
					{
						lines.add(line);
					}
					br.close();
					ReducerMinionPool rpool = new ReducerMinionPool(lines,Integer.parseInt(numThreads),directory+op_dir, job);
					rpool.sync_all();
					//logger.debug("Port:"+worker_port+" startin reduce");
					worker_status="idle";
					
					Socket client= null;
					try {
						client = new Socket(master_address,master_port);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					PrintWriter out=null;

					try {
						out = new PrintWriter(client.getOutputStream(),true);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					out.println("GET /workerstatus?keysRead="+keysRead+""+"&keysWritten="+keysWritten+""+"&job="+job+"&status="+worker_status+"&port="+worker_port+" HTTP/1.1\r\nHost: "+master_address.getHostAddress()+"\r\nConnection: close\r\n");

					//out.println("GET /HW3/workerstatus?keysRead=10&keysWritten=20&job="+job+"&status="+worker_status+"&port="+worker_port+" HTTP/1.1\r\nHost: "+master_address.getHostAddress()+"\r\nConnection: close\r\n");
					out.close();
					client.close();
					
					
					
					
					
				}
				
				
			}
	void delete(File f)
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
	
	static synchronized void inc_read()
	{
		keysRead++;
	}
	static synchronized void in_wr()
	{
		keysWritten++;
	}
	
}

