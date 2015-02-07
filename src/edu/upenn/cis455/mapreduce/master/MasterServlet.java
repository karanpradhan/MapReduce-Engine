package edu.upenn.cis455.mapreduce.master;

import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.TimeZone;

import javax.servlet.*;
import javax.servlet.http.*;

public class MasterServlet extends HttpServlet {

	static final long serialVersionUID = 455555001;
	HashMap<String,WorkerData> worker_status ;
	Object lock;
	//Controller c;
	public void init(ServletConfig config) throws ServletException
	{
		//this.config = config;
		super.init(config);
		worker_status = new HashMap<String,WorkerData>();
		
		lock = new Object();
		//c = new Controller(lock);
	}
	
	String printMap()
	{
		String s = "";
		for(String x:worker_status.keySet())
		{
			s=s+","+x+" -> "+worker_status.get(x).toString();
		}
		return s;
	}
	
	int getWaitCount()
	{
		int i = 0;
		for(String key:worker_status.keySet())
		{
			if(worker_status.get(key).getStatus().equals("waiting"))
			{
				i ++ ;
			}
		}
		return i;
		
	}
	
	int getIdleCount()
	{
		int i = 0;
		for(String key:worker_status.keySet())
		{
			if(worker_status.get(key).getStatus().equals("idle"))
			{
				i ++ ;
			}
		}
		return i;
		
	}
	

	void sync_all(int active)
	{
		synchronized(lock)
		{
			while(getWaitCount()<active)
			try {
				lock.wait(10);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		//notifyAll();
		}
		
	}
	
	
	void sync_all_2(int active)
	{
		synchronized(lock)
		{
			while(getIdleCount()<active)
			try {
				lock.wait(10);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		//notifyAll();
		}
		
	}
	

	public void doGet(HttpServletRequest request, HttpServletResponse response) 
			throws java.io.IOException
			{
		String url = request.getServletPath();
		if(url.startsWith("/workerstatus"))
		{
			String keys_read = request.getParameter("keysRead");
			String keys_written = request.getParameter("keysWritten");
			String job = request.getParameter("job");
			String status = request.getParameter("status");
			String ip_address = request.getRemoteAddr();
			int port = Integer.parseInt(request.getParameter("port"));
			Calendar now = Calendar.getInstance();
			SimpleDateFormat df = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z",Locale.US);
			df.setTimeZone(TimeZone.getTimeZone("GMT"));
			String time = df.format(now.getTime());
			WorkerData w_data = new WorkerData(Integer.parseInt(keys_read),Integer.parseInt(keys_written),time,job,status);
			//String worker_value = "keysRead="+keys_read+";keysWritten="+keys_written+";job="+job+";status="+status+";time="+time;
			String worker_key = ip_address+":"+port;
			worker_status.put(worker_key,w_data);
			System.out.println("Worker status: "+printMap());
			//wake_up();
		}
		else
		{
			HashSet<String> active = new HashSet<String>();
			response.setContentType("text/html");
			PrintWriter out = response.getWriter();
			out.println("<html><body><h2>Name: Karan Pradhan</h2><h2>SEAS login: karanpr</h2><h3>Master Control</h3>");
			if(!worker_status.isEmpty())
			{

				
				Calendar now = Calendar.getInstance();
				SimpleDateFormat df = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z",Locale.US);
				df.setTimeZone(TimeZone.getTimeZone("GMT"));
				String time_now = df.format(now.getTime());
				long current = 0;
				try {
					current = df.parse(time_now).getTime();
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				for(String key:worker_status.keySet())
				{
					long worker_time = 0;
					try {
						worker_time = df.parse(worker_status.get(key).getTime()).getTime();
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					if(current-worker_time<=30000)
					{
						active.add(key);
					}
				}
				if(active.isEmpty())
				{
					out.println("<h4>No active workers!</h4>");
				}
				else
				{
					out.println("<table border='1'><th>IP:Port</th><th>Status</th><th>Job</th><th>Keys Read</th><th>Keys Written</th>");

					for(String key : active)
					{
						out.println("<tr>");
						out.println("<td>"+key+"</td>");
						out.println("<td>"+worker_status.get(key).getStatus()+"</td>");
						out.println("<td>"+worker_status.get(key).getJob()+"</td>");
						out.println("<td>"+worker_status.get(key).getKeysRead()+"</td>");
						out.println("<td>"+worker_status.get(key).getKeysWritten()+"</td>");
						out.println("</tr>");
					}

					out.println("</table>");
				}
			}
			else
			{

				out.println("<h4>No workers online yet</h4>");
			}
			String s = "";
			for(String key : active)
			{
				s = s+key+";";
			}
			out.println("<form action='./temp' method = 'post'>");
			out.println("<p>Job Name: <Input type='text' name='job'/></p>");
			out.println("<p>Input Directory: <Input type='text' name='ip_dir'/></p>");
			out.println("<p>Output Directory: <Input type='text' name='op_dir'/></p>");
			out.println("<p>Number of Map Threads: <Input type='text' name='map_threads'/></p>");
			out.println("<p>Number of Reduce Threads: <Input type='text' name='reduce_threads'/></p>");
			out.println("<Input type='hidden' name='active' value='"+active.size()+"'/>");
			out.println("<Input type='hidden' name='workers' value='"+s+"'/>");
			out.println("<p><Input type='submit' value='Issue Job!!'></p>");
			out.println("</form></body></html>");


		}

			}

	public void doPost(HttpServletRequest request, HttpServletResponse response) throws java.io.IOException
	{
		String job = request.getParameter("job");
		String ip_dir = request.getParameter("ip_dir");
		String op_dir = request.getParameter("op_dir");
		String no_map_threads = request.getParameter("map_threads");
		String no_red_threads = request.getParameter("reduce_threads");
		String no_active = request.getParameter("active");
		String workers = request.getParameter("workers");
		if(!(job!=null && !job.equals("") && ip_dir!=null && !ip_dir.equals("") && op_dir!=null && !op_dir.equals("") && no_map_threads!=null && !no_map_threads.equals("") && no_red_threads!=null && !no_red_threads.equals("") && no_active!=null && !no_active.equals("")))
		{
			response.setContentType("text/html");
			PrintWriter out = response.getWriter();
			out.println("<html><body><h3>Invalid entries in the form !<h3><a href='./status'>Click here to try again</a></body></html>");
		}
		else
		{
			int lim = Integer.parseInt(no_active);
			String active_worker_list[] = workers.split(";");
			String worker_message = "";
			for(int j=0;j<lim;j++)
			{
				if(lim!=1)
					worker_message = worker_message +"worker"+(j+1)+"="+active_worker_list[j] + "&";
				else
					worker_message = worker_message +"worker"+(j+1)+"="+active_worker_list[j] ;
			}
			//System.out.println(lim);
			for(int i=0;i<lim;i++)
			{
				if(worker_status.get(active_worker_list[i]).getStatus().equals("idle"))
				{
				String temp[] = active_worker_list[i].split(":");
				String ip = temp[0];
				int port = Integer.parseInt(temp[1]);
				Socket client= null;
				try {
					client = new Socket(InetAddress.getByName(ip),port);
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
				String message = "job="+job+"&input="+ip_dir+"&numThreads="+no_map_threads+"&numWorkers="+no_active+"&"+worker_message;
				out.print("POST /runmap HTTP/1.1\r\n");
				out.print("Host: "+ip+"\r\n");
				out.print("Content-Length: "+message.length()+"\r\n");
				out.print("Content-Type: application/x-www-form-urlencoded\r\n");
				out.print("Connection: close\r\n\r\n");
				out.print(message);
				out.close();
				client.close();
				
				}
			}
			
			System.out.println("wait count :" + getWaitCount());
			sync_all(lim);
			/*while(getWaitCount()<=lim)
			{
				//System.out.println(getWaitCount());
				//System.out.println("waiting...");
				if(getWaitCount()>=lim)
					break;
			}*/
			System.out.println("Done sync with "+ getWaitCount());
			for(int i=0;i<lim;i++)
			{
				if(worker_status.get(active_worker_list[i]).getStatus().equals("waiting"))
				{
				String temp[] = active_worker_list[i].split(":");
				String ip = temp[0];
				int port = Integer.parseInt(temp[1]);
				Socket client= null;
				try {
					client = new Socket(InetAddress.getByName(ip),port);
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
				String message = "job="+job+"&output="+op_dir+"&numThreads="+no_red_threads+"&numWorkers="+no_active+"&"+worker_message;
				out.print("POST /runreduce HTTP/1.1\r\n");
				out.print("Host: "+ip+"\r\n");
				out.print("Content-Length: "+message.length()+"\r\n");
				out.print("Content-Type: application/x-www-form-urlencoded\r\n");
				out.print("Connection: close\r\n\r\n");
				out.print(message);
				out.close();
				client.close();
				System.out.println("Sent reduce to "+active_worker_list[i]);
				}
				
				
				
				
			}
			sync_all_2(lim);
			
			PrintWriter printWriter = response.getWriter();
			response.setContentType("text/html");
			printWriter.print("<html><body><h3>Your output is ready </h3>Click <a href='./status'>here </a>to issue a new job</body>");
			
			
			
		}

	}


}

