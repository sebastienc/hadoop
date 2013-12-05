package org.apache.hadoop.mapred;

import org.apache.log4j.AsyncAppender;
import org.apache.log4j.net.SocketAppender;
import org.apache.log4j.spi.LoggingEvent;

public class TaskSocketAppender extends AsyncAppender {

	static final String TASKID_PROPERTY = "hadoop.tasklog.taskid";

	private final TaskAttemptID taskId;

	/** SocketAppender properties */
	private int port;

	private String remoteHost;

	static final TaskAttemptID extractTaskId() {
		String str = System.getProperty(TASKID_PROPERTY);
		if (str != null) {
			return TaskAttemptID.forName(str);
		} else {
			return null;
		}
	}

	public TaskSocketAppender() {
		taskId = extractTaskId();
		super.setBlocking(false);
	}

	public String getRemoteHost() {
		return remoteHost;
	}

	public void setRemoteHost(String remoteHost) {
		this.remoteHost = remoteHost;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	@Override
	public void activateOptions() {
		addAppender(new SocketAppender(remoteHost, port));
		addAppender(newTaskLogAppender());
		super.activateOptions();
	}

	@Override
	public void append(LoggingEvent event) {
		if (event == null)
			return;
		if (taskId != null) {
			event.setProperty("task_attempt_id", taskId.toString());
			event.setProperty("task_id", taskId.getTaskID().toString());
			event.setProperty("job_id", taskId.getJobID().toString());
		}
		super.append(event);
	}

	private TaskLogAppender newTaskLogAppender() {
		TaskLogAppender tla = new TaskLogAppender();
		tla.setLayout(getLayout());
		tla.activateOptions();
		return tla;
	}
}
