/**
 * 
 */
package org.bitsoftware.aws.task;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.util.StringUtils;

/**
 * S3 create folder task
 * 
 * @author Robert Hindli
 * @date Apr 20, 2017
 *
 */
class S3CreateFolderTask extends AbstractTask
{
	public static final String TaskName = "s3createfolder";

	private String p_awsAccessKey;
	private String p_awsSecretKey;
	private String p_awsRegionName;
	private String p_awsBucketName;
	private String p_folderName;
	
	/** Constructor */
	public S3CreateFolderTask(String[] params) throws InvalidTaskParamException
	{
		super(params);
	}

	@Override
	protected String getParamsUsage()
	{
		String retVal =
	    	"Valid parameters for running " + TaskName + " task:\n" +
	    	"-a:<awsaccesskey>          : AWS access key.\n" +
	    	"-s:<awssecretkey>          : AWS secret key.\n" +
	    	"-r:<awsregionname>         : AWS region name. E.g. eu-west-1, eu-central-1\n" +
	    	"-b:<bucket>                : S3 bucket where file will be uploaded.\n" +
	    	"-f:<foldername>            : Folder name.\n";
		
		return retVal;
	}

	@Override
	protected void validateParams() throws InvalidTaskParamException
	{
		for(String par : params.keySet())
		{
			switch(par)
			{
			case "-a":
				p_awsAccessKey = params.get(par);
				break;
			case "-s":
				p_awsSecretKey = params.get(par);
				break;
			case "-r":
				p_awsRegionName = params.get(par);
				break;
			case "-b":
				p_awsBucketName = params.get(par);
				break;
			case "-f":
				p_folderName = params.get(par);
				break;
			}
		}
		
		if(StringUtils.isNullOrEmpty(p_awsAccessKey))
		{
			String err = "Missing AWS access key parameter. See usage.";
			String helpMsg = getParamsUsage();
			throw new InvalidTaskParamException(err, helpMsg);
		}

		if(StringUtils.isNullOrEmpty(p_awsSecretKey))
		{
			String err = "Missing AWS secret key parameter. See usage.";
			String helpMsg = getParamsUsage();
			throw new InvalidTaskParamException(err, helpMsg);
		}

		if(StringUtils.isNullOrEmpty(p_awsRegionName))
		{
			String err = "Missing AWS region name parameter. See usage.";
			String helpMsg = getParamsUsage();
			throw new InvalidTaskParamException(err, helpMsg);
		}
		else
		{
			Region awsRegion = null;
			
			try
			{
				awsRegion = RegionUtils.getRegion(p_awsRegionName);
			}
			catch(Exception e)
			{
				
			}
			
			if(awsRegion == null)
			{
				throw new InvalidTaskParamException("Invalid AWS region name.");
			}
		}

		if(StringUtils.isNullOrEmpty(p_awsBucketName))
		{
			String err = "Missing bucket name parameter. See usage.";
			String helpMsg = getParamsUsage();
			throw new InvalidTaskParamException(err, helpMsg);
		}

		if(StringUtils.isNullOrEmpty(p_folderName))
		{
			String err = "Missing folder name parameter. See usage.";
			String helpMsg = getParamsUsage();
			throw new InvalidTaskParamException(err, helpMsg);
		}
	}

	@Override
	public void runImpl()
	{
		BasicAWSCredentials awsCreds = new BasicAWSCredentials(p_awsAccessKey, p_awsSecretKey);
		AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
		                        .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
		                        .withRegion(p_awsRegionName)
		                        .build();

		String folderKeyName = p_folderName + "/";

		// Create empty content
        InputStream emptyContent = new ByteArrayInputStream(new byte[0]);

        //Create object metadata
		ObjectMetadata om = new ObjectMetadata();

        PutObjectRequest por = new PutObjectRequest(p_awsBucketName, folderKeyName, emptyContent, om);

        try 
        {
    		s3Client.putObject(por);
        }
        catch (Exception e)
        {
            System.err.println(e.getMessage());
        }
	}

	@Override
	public String getDescription()
	{
		return "Creating folder \"" + p_folderName + "\" in bucket \"" + p_awsBucketName + "\"";
	}
}
