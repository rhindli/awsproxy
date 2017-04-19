/**
 * 
 */
package org.bitsoftware.aws.util;

/**
 * @author Robert Hindli
 * @date Apr 18, 2017
 *
 */
public class Utils
{
	public final static long ONE_SECOND = 1000;
	public final static long SECONDS = 60;

	public final static long ONE_MINUTE = ONE_SECOND * 60;
	public final static long MINUTES = 60;

	public final static long ONE_HOUR = ONE_MINUTE * 60;
	public final static long HOURS = 24;

	public static String printDurationFromMillis(long duration)
	{
		StringBuffer res = new StringBuffer();
		long temp = 0;
		if (duration >= ONE_SECOND)
		{
			temp = duration / ONE_HOUR;
			if (temp > 0)
			{
				duration -= temp * ONE_HOUR;
				res.append(temp).append(" hour").append(temp > 1 ? "s" : "").append(duration >= ONE_MINUTE ? ", " : "");
			}

			temp = duration / ONE_MINUTE;
			if (temp > 0)
			{
				duration -= temp * ONE_MINUTE;
				res.append(temp).append(" minute").append(temp > 1 ? "s" : "");
			}

			if (!res.toString().equals("") && duration >= ONE_SECOND)
			{
				res.append(" and ");
			}

			temp = duration / ONE_SECOND;
			if (temp > 0)
			{
				res.append(temp).append(" second").append(temp > 1 ? "s" : "");
			}
			return res.toString();
		}
		else
		{
			return duration + " milliseconds";
		}
	}

}
