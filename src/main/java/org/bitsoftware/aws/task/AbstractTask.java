/**
 * 
 */
package org.bitsoftware.aws.task;

import java.util.HashMap;

import org.bitsoftware.aws.Task;

/**
 * @author Robert Hindli
 * @date Apr 13, 2017
 *
 */
abstract class AbstractTask implements Task
{
	/** Task parameters <paramName, paramValue> */
	protected HashMap<String, String> params = new HashMap<>();
	
	public AbstractTask(String[] params)
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
	
	protected abstract void printParamsUsage();
	
	protected abstract void validateParams();

}
