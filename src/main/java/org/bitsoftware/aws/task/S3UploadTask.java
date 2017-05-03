/**
 * 
 */
package org.bitsoftware.aws.task;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
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
	private String p_awsDirectoryPath;
	private String p_acl;
	private File p_file;
	
	/** Constructor */
	public S3UploadTask(String[] params) throws InvalidTaskParamException
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
    		"-b:<bucket>                : S3 bucket where file(s) will be uploaded.\n" +
    		"                             Bucker must already exists in S3.\n" +
    		"[-d:<directorypath>]       : Optional S3 folder path under specified bucket\n" +
    		"                             where file(s) will be uploaded.\n" +
    		"                             Any missing folder will be created automatically.\n" +
    		"-f:<file or folder>        : Path to the file(s) to upload.\n" +
    		"                             If this is a folder, all files in that folder will be uploaded\n" +
    		"[-acl:public-read]         : Optional access control.\n" +
    		"                             public-read: public read";
    	
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
			case "-d":
				p_awsDirectoryPath = params.get(par);
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

		if(!StringUtils.isNullOrEmpty(p_awsDirectoryPath))
		{
			p_awsDirectoryPath = p_awsDirectoryPath.trim();
			
			if(!StringUtils.isNullOrEmpty(p_awsDirectoryPath))
			{
				//remove any / from the beginning
				p_awsDirectoryPath = p_awsDirectoryPath.replace("^/+", "");

				//remove any number of / from the end
				p_awsDirectoryPath = p_awsDirectoryPath.replace("/+$", "");
			}
		}

		if(p_file == null)
		{
			String err = "Missing file parameter. See usage.";
			String helpMsg = getParamsUsage();
			throw new InvalidTaskParamException(err, helpMsg);
		}
		else if(!p_file.exists())
		{
			throw new InvalidTaskParamException("File does not exists.");
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

		ArrayList<File> files2upload = new ArrayList<>();

		if(p_file.isDirectory())
		{
			try (DirectoryStream<Path> stream = Files.newDirectoryStream(p_file.toPath()))
			{
				for (Path entry : stream)
				{
					File file2upload = entry.toFile();
					if(file2upload.isFile())
						files2upload.add(file2upload);
				}
			}
			catch (DirectoryIteratorException e)
			{
				System.err.println(e.getCause().getMessage());
				return;
			}
			catch(IOException e)
			{
				System.err.println(e.getMessage());
				return;
			}

			for(File f : files2upload)
			{
		    	long start = System.currentTimeMillis();
				System.out.println("    Uploading file \"" + f.getName() + "\" ...");

				uploadFile(s3Client, f);

		    	long duration = System.currentTimeMillis() - start;
		    	String totalDuration = Utils.printDurationFromMillis(duration);
				System.out.println("    Completed in " + totalDuration + ".");
			}
		}
		else
			uploadFile(s3Client, p_file);
	       
	}
	
	private void uploadFile(AmazonS3 s3Client, File file)
	{
		String fileKeyName = "";
		
		if(!StringUtils.isNullOrEmpty(p_awsDirectoryPath))
			fileKeyName += (p_awsDirectoryPath + "/");
		
		fileKeyName += file.getName();

		PutObjectRequest por = new PutObjectRequest(p_awsBucketName, fileKeyName, file);
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
    		Upload upload = tm.upload(por);
            
    		upload.waitForCompletion();
        }
        catch (Exception e)
        {
            System.err.println(e.getMessage());
        }
        finally
        {
        	tm.shutdownNow(false);
        }
	}
	
	@Override
	public String getDescription()
	{
		if(p_file.isDirectory())
			return "Uploading files from folder \"" + p_file.getAbsolutePath() + "\"";
		
		return "Uploading file \"" + p_file.getName() + "\"";
	}
}
