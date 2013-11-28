package org.apache.hadoop.mapred;

import java.net.InetAddress;

import org.apache.log4j.net.SocketAppender;
import org.apache.log4j.spi.LoggingEvent;

public class TaskSocketAppender extends SocketAppender {

	static final String TASKID_PROPERTY = "hadoop.tasklog.taskid";

	private final TaskAttemptID taskId;

	public TaskSocketAppender() {
		taskId = extractTaskId();
	}
	
	public TaskSocketAppender(InetAddress address, int port) {
		  super(address, port);
			taskId = extractTaskId();
	}
	
	public TaskSocketAppender(String host, int port) {
		super(host, port);
		taskId = extractTaskId();
	}

	static final TaskAttemptID extractTaskId() {
		String str = System.getProperty(TASKID_PROPERTY);
		if(str != null) {
		    return TaskAttemptID.forName(str);
		} else {
			return null;
		}
	}

	@Override
	public void append(LoggingEvent event) {
		if(event == null)
		      return;
		if(taskId != null) {
		  event.setProperty("task_attempt_id", taskId.toString());
		  event.setProperty("task_id", taskId.getTaskID().toString());
		  event.setProperty("job_id", taskId.getJobID().toString());
		}
		super.append(event);
	}
}
