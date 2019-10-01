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
	private boolean p_recursive;
	
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
    		"                             If not specified the file(s) will be uploaded directly under the bucket.\n" +
    		"                             Any missing folder will be created automatically.\n" +
    		"-f:<file or folder>        : Path to the file(s) to upload.\n" +
    		"                             If this is a folder, all files in that folder will be uploaded\n" +
    		"[-t[:false|true]           : Optional indication to upload the whole file tree under the specified folder.\n" +
    		"                             Used only if uploading a folder.\n" +
    		"                             Ignored if uploading a file.\n" +
    		"                             Default value if not specified is false.\n" +
    		"                             Default value if specified without true or false indication is true.\n" +
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
			case "-t":
				String v = params.get(par);
				if(v == null)
					p_recursive = true;
				else
					p_recursive = Boolean.valueOf(v);
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

		p_awsDirectoryPath = normalizePath(p_awsDirectoryPath);

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

		ArrayList<File2Upload> files2Upload = getFiles2Upload(p_file, 0);

		if(files2Upload != null)
		{
			for(File2Upload f : files2Upload)
			{
		    	long start = System.currentTimeMillis();

		    	if(f.Level > 0)
				{
		    		String RelativePath = (StringUtils.hasValue(f.RelativePath) ? f.RelativePath + "/" : ""); 
		    		System.out.println("    Uploading file \"" + RelativePath + f.File.getName() + "\" ...");
				}
	
				uploadFile(s3Client, f.File, f.RelativePath);
	
		    	if(f.Level > 0)
		    	{
			    	long duration = System.currentTimeMillis() - start;
			    	String totalDuration = Utils.printDurationFromMillis(duration);
					System.out.println("    Completed in " + totalDuration + ".");
		    	}
			}
		}
	       
	}
	
	private ArrayList<File2Upload> getFiles2Upload(File file, int level)
	{
		ArrayList<File2Upload> files2upload = new ArrayList<>();

		if(file.isFile())
		{
			String rootDirPath = p_file.getAbsolutePath();
			if(p_file.isFile())
				rootDirPath = p_file.getParentFile().getAbsolutePath();

			String filePath = file.getParentFile().getAbsolutePath();
			String relativePath = filePath.replace(rootDirPath, "");

			relativePath = normalizePath(relativePath);
			
			File2Upload file2Upload = new File2Upload(file, relativePath, level);
			files2upload.add(file2Upload);
		}
		else if(level == 0 || p_recursive)
		{
			try (DirectoryStream<Path> stream = Files.newDirectoryStream(file.toPath()))
			{
				for (Path entry : stream)
				{
					File fileEntry = entry.toFile();
					ArrayList<File2Upload> dirFiles2Upload = getFiles2Upload(fileEntry, level+1);
					files2upload.addAll(dirFiles2Upload);
				}
			}
			catch (DirectoryIteratorException e)
			{
				System.err.println(e.getCause().getMessage());
				return null;
			}
			catch(IOException e)
			{
				System.err.println(e.getMessage());
				return null;
			}
		}
		
		return files2upload;
	}
	
	private void uploadFile(AmazonS3 s3Client, File file, String relativePath)
	{
		String fileKeyName = "";
		
		if(!StringUtils.isNullOrEmpty(p_awsDirectoryPath))
			fileKeyName += (p_awsDirectoryPath + "/");
		
		if(!StringUtils.isNullOrEmpty(relativePath))
			fileKeyName += (relativePath + "/");
		
		fileKeyName += file.getName();

		PutObjectRequest por = new PutObjectRequest(p_awsBucketName, fileKeyName, file);
		ObjectMetadata om = new ObjectMetadata();
		por.setMetadata(om);

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
		String awsDestination = p_awsBucketName +
				(StringUtils.hasValue(p_awsDirectoryPath) ? "/" + p_awsDirectoryPath : "");
		
		if(p_file.isDirectory())
			return "Uploading files from folder \"" + p_file.getAbsolutePath() + "\" to \"" + awsDestination + "\"";
		
		return "Uploading file \"" + p_file.getName() + "\" to \"" + awsDestination + "\"";
	}
	

	private String normalizePath(String path)
	{
		if(path != null)
			path = path.trim();
		
		if(StringUtils.isNullOrEmpty(path))
			return path;
		
		path = path.replace("\\", "/");
		
		//remove any / from the beginning
		path = path.replaceAll("^/+", "");

		//remove any number of / from the end
		path = path.replaceAll("/+$", "");
		
		return path;
	}
	
	private static class File2Upload
	{
		File File;
		String RelativePath;
		int Level;

		File2Upload(File f, String relativePath, int level)
		{
			this.File = f;
			this.RelativePath = relativePath;
			this.Level = level;
		}
		
	}

}
