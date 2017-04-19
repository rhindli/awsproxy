/**
 * 
 */
package org.bitsoftware.aws.task;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.bitsoftware.aws.util.Utils;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.ExecutorFactory;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.GroupGrantee;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.Permission;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.util.StringUtils;

/**
 * S3 upload task
 * 
 * @author Robert Hindli
 * @date Apr 13, 2017
 *
 */
class S3UploadTask extends AbstractTask
{
	public static final String TaskName = "s3upload";

	private static final String ACL_Public_Read = "public-read";
	
	private String p_awsAccessKey;
	private String p_awsSecretKey;
	private String p_awsRegionName;
	private String p_awsBucketName;
	private String p_acl;
	private File p_file;
	
	/** Constructor */
	public S3UploadTask(String[] params)
	{
		super(params);
	}

	@Override
	protected void printParamsUsage()
	{
    	System.out.println("Valid parameters for running " + TaskName + " task:");
    	System.out.println("-a:<awsaccesskey>          : AWS access key.");
    	System.out.println("-s:<awssecretkey>          : AWS secret key.");
    	System.out.println("-r:<awsregionname>         : AWS region name. E.g. eu-west-1, eu-central-1");
    	System.out.println("-b:<bucket>                : S3 bucket where file will be uploaded.");
    	System.out.println("-f:<file>                  : Path to the file to upload.");
    	System.out.println("[-acl:public-read]         : Optional access control.");
    	System.out.println("                             public-read: public read");
	}

	@Override
	protected void validateParams()
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
				p_file = new File(params.get(par));
				break;
			case "-acl":
				p_acl = params.get(par);
				break;
			}
		}
		
		if(StringUtils.isNullOrEmpty(p_awsAccessKey))
		{
			System.err.println("Missing AWS access key parameter. See usage.");
			System.err.println();
			printParamsUsage();
			System.exit(1);
		}

		if(StringUtils.isNullOrEmpty(p_awsSecretKey))
		{
			System.err.println("Missing AWS secret key parameter. See usage.");
			System.err.println();
			printParamsUsage();
			System.exit(1);
		}

		if(StringUtils.isNullOrEmpty(p_awsRegionName))
		{
			System.err.println("Missing AWS secret key parameter. See usage.");
			System.err.println();
			printParamsUsage();
			System.exit(1);
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
				System.err.println("Invalid AWS region name.");
				System.err.println();
				System.exit(1);
			}
		}

		if(StringUtils.isNullOrEmpty(p_awsBucketName))
		{
			System.err.println("Missing bucket name parameter. See usage.");
			System.err.println();
			printParamsUsage();
			System.exit(1);
		}

		if(p_file == null)
		{
			System.err.println("Missing file parameter. See usage.");
			System.err.println();
			printParamsUsage();
			System.exit(1);
		}
		else if(!p_file.exists())
		{
			System.err.println("File does not exists.");
			System.err.println();
			printParamsUsage();
			System.exit(1);
		}
		else if(!p_file.isFile())
		{
			System.err.println("File parameter does not point to a file.");
			System.err.println();
			printParamsUsage();
			System.exit(1);
		}
	}

	@Override
	public void run()
	{
		BasicAWSCredentials awsCreds = new BasicAWSCredentials(p_awsAccessKey, p_awsSecretKey);
		AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
		                        .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
		                        .withRegion(p_awsRegionName)
		                        .build();

		String fileKeyName = p_file.getName();

		PutObjectRequest por = new PutObjectRequest(p_awsBucketName, fileKeyName, p_file);
		ObjectMetadata om = new ObjectMetadata();
		por.setMetadata(om);
		por.setStorageClass(StorageClass.ReducedRedundancy);

		AccessControlList acl = new AccessControlList();

		if(!StringUtils.isNullOrEmpty(p_acl))
        {
    		if(ACL_Public_Read.equalsIgnoreCase(p_acl))
        	{
        		acl.grantPermission(GroupGrantee.AllUsers, Permission.Read);
        	}
        }

		if(!acl.getGrantsAsList().isEmpty())
		{
			por.setAccessControlList(acl);
		}

		TransferManager tm = TransferManagerBuilder.standard()
				.withS3Client(s3Client)
				.withExecutorFactory(new ExecutorFactory()
				{
					@Override
					public ExecutorService newExecutor()
					{
						return Executors.newFixedThreadPool(20);
					}
				})
				.build()
				;

        try 
        {
        	long start = System.currentTimeMillis();
    		System.out.println("Upload started for file " + "\"" + p_file.getName() + "\"");
        	
    		Upload upload = tm.upload(por);
            
    		upload.waitForCompletion();
    		
        	long duration = System.currentTimeMillis() - start;
        	String totalDuration = Utils.printDurationFromMillis(duration);
    		System.out.println("Upload completed in " + totalDuration);
        }
        catch (Exception e)
        {
            System.err.println(e.getMessage());
        }
        finally
        {
        	tm.shutdownNow();
        }
	}
}
