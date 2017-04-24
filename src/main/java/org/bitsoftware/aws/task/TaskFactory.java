/**
 * 
 */
package org.bitsoftware.aws.task;

import org.bitsoftware.aws.Task;

/**
 * Task Factory
 * 
 * @author Robert Hindli
 * @date Apr 13, 2017
 *
 */
public class TaskFactory
{

	private static final TaskFactory instance = new TaskFactory();
			
	private TaskFactory()
	{

	}

	public static TaskFactory getInstance()
	{
		return instance;
	}
	
	public Task getTask(String taskName, String[] taskParams) throws InvalidTaskParamException
	{
		if(S3UploadTask.TaskName.equalsIgnoreCase(taskName))
		{
			return new S3UploadTask(taskParams);
		}

		if(S3CreateFolderTask.TaskName.equalsIgnoreCase(taskName))
		{
			return new S3CreateFolderTask(taskParams);
		}

		throw new UnsupportedOperationException("Task " + taskName + " is not implemented");
	}
}
