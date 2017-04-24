/**
 * 
 */
package org.bitsoftware.aws.task;

/**
 * Invalid task parameter
 * 
 * @author Robert Hindli
 * @date Apr 20, 2017
 *
 */
public class InvalidTaskParamException extends Exception
{
	/** Serial version ID */
	private static final long serialVersionUID = 1207511036233802927L;

	private String helpMsg;

	/** Constructor */
	public InvalidTaskParamException(String message)
	{
		super(message);
	}

	/** Constructor */
	public InvalidTaskParamException(Throwable cause)
	{
		super(cause);
	}

	/** Constructor */
	public InvalidTaskParamException(String message, Throwable cause)
	{
		super(message, cause);
	}

	/** Constructor */
	public InvalidTaskParamException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace)
	{
		super(message, cause, enableSuppression, writableStackTrace);
	}
	
	/** Constructor */
	public InvalidTaskParamException(String message, String helpMsg)
	{
		super(message);
		this.helpMsg = helpMsg;
	}

	/** Get help message */
	public String getHelpMsg()
	{
		return helpMsg;
	}
	
	/** Set help message */
	public void setHelpMsg(String helpMsg)
	{
		this.helpMsg = helpMsg;
	}
}
