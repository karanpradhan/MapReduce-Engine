package edu.upenn.cis455.mapreduce.master;

public class WorkerData {
	private int keysRead;
	private int keysWritten;
	private String time;
	private String job;
	private String status;
	
	WorkerData(int k_read,int k_write,String time,String job,String status)
	{
		setKeysRead(k_read);
		setKeysWritten(k_write);
		this.setTime(time);
		this.setJob(job);
		this.setStatus(status);
		
	}

	public int getKeysRead() {
		return keysRead;
	}

	public void setKeysRead(int keysRead) {
		this.keysRead = keysRead;
	}

	public int getKeysWritten() {
		return keysWritten;
	}

	public void setKeysWritten(int keysWritten) {
		this.keysWritten = keysWritten;
	}

	public String getTime() {
		return time;
	}

	public void setTime(String time) {
		this.time = time;
	}

	public String getJob() {
		return job;
	}

	public void setJob(String job) {
		this.job = job;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}
	public String toString()
	{
		return "kread:"+keysRead+"kwrite:"+keysWritten+"job:"+job+"status:"+status;
	}

}
