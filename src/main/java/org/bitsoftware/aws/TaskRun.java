package org.bitsoftware.aws;

import org.bitsoftware.aws.task.TaskFactory;

/**
 * Hello world!
 *
 */
public class TaskRun 
{
    public static void main( String[] args )
    {
    	if(args.length < 1)
    	{
    		System.err.println("No task specified. Please see usage.");
    		printUsage();
    		System.exit(1);
    	}
    	
    	String taskName = args[0];
    	String[] taskParams = new String[args.length - 1];
    	
    	if(args.length > 1)
    		System.arraycopy(args, 1, taskParams, 0, taskParams.length);
    	
    	Task task = TaskFactory.getInstance().getTask(taskName, taskParams);
    	task.run();
    }
    
    private static void printUsage()
    {
    	System.out.println("java -jar awsproxy.jar <task> [<task params>]");
    }
}
