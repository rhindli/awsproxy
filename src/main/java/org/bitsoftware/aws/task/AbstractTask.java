/**
 * 
 */
package org.bitsoftware.aws.task;

import java.util.HashMap;

import org.bitsoftware.aws.Task;
import org.bitsoftware.aws.util.Utils;

/**
 * @author Robert Hindli
 * @date Apr 13, 2017
 *
 */
abstract class AbstractTask implements Task
{
	/** Task parameters <paramName, paramValue> */
	protected HashMap<String, String> params = new HashMap<>();
	
	public AbstractTask(String[] params) throws InvalidTaskParamException
	{
		parseParams(params);
		validateParams();
	}

	private void parseParams(String[] params)
	{
		for(int i=0; i < params.length; i++)
		{
			String param = params[i];
			
			int idx = param.indexOf(":");
			
			if(idx > 0)
				this.params.put(param.substring(0, idx), param.substring(idx+1));
			else
				this.params.put(param, null);
		}
	}
	
	protected abstract String getParamsUsage();
	
	protected abstract void validateParams() throws InvalidTaskParamException;

	public void run()
	{
    	long start = System.currentTimeMillis();
		System.out.println(getDescription() + " ...");
    	
    	
    	runImpl();

    	long duration = System.currentTimeMillis() - start;
    	String totalDuration = Utils.printDurationFromMillis(duration);
		System.out.println("Completed in " + totalDuration + ".");

	}
	
	protected abstract void runImpl();
	
	protected abstract String getDescription();
}
