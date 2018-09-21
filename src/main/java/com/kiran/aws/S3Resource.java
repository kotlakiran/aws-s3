package com.kiran.aws;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;



@RestController
public class S3Resource {

	@Value("${aws.s3.bucketName}")
	private String bucketName;

	private AmazonS3 s3;
	
	public S3Resource(@Value("${aws.accessKey}") String accessKey,
			@Value("${aws.secretKey}") String secretKey) {
		try {
			com.amazonaws.auth.BasicAWSCredentials credentials = new com.amazonaws.auth.BasicAWSCredentials(
					accessKey, secretKey);
			s3 = AmazonS3ClientBuilder.standard()
            .withCredentials(new com.amazonaws.auth.AWSStaticCredentialsProvider(credentials))
            .withRegion(Regions.US_WEST_2)
            .build();
		} catch (Exception ex) {
			throw new com.amazonaws.AmazonClientException(
					"Cannot load the credentials from the credential profiles file. "
							+ "Please make sure that your credentials file is at the correct. ",
					ex);
		}
	}
	
	
	
	@RequestMapping("/uploadFolder")
	public List<String>  test(@RequestParam(value="folderPath")String folderPath,@RequestParam(value="awsfolderPath")String awsfolderPath,@RequestParam(value="withCannedAcl")CannedAccessControlList withCannedAcl) throws IOException {
		System.out.println("Reading files from  ="+folderPath);
		List<String> publicUrls=new ArrayList<>();
		
		System.out.println("withCannedAcl ="+withCannedAcl);
		try (Stream<Path> paths = Files.walk(Paths.get(folderPath))) {
		    paths
		        .filter(Files::isRegularFile)
		        .forEach(file->{
		        
		        PutObjectRequest request = new PutObjectRequest(bucketName, awsfolderPath+IOUtils.DIR_SEPARATOR+file.getFileName().toString(), new File(file.toAbsolutePath().toString()));
		    	request.withCannedAcl(withCannedAcl);
		    	publicUrls.add(uploadToS3(request));
		        });
		}
		return publicUrls;
		

	}
	
	private String  uploadToS3( PutObjectRequest request) {
		
		List<String> publicUrls=new ArrayList<>();
		// Added for Server-Side Encryption
		ObjectMetadata objectMetadata = new com.amazonaws.services.s3.model.ObjectMetadata();
		objectMetadata
				.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
		Map<String, String> imageMetadata = new HashMap<>();
		
		imageMetadata.put("vendor", "GSIS");
		imageMetadata.put("vendorImageCategory", "headshots");
		imageMetadata.put("lastUpdatedTimestamp",
				String.valueOf(System.currentTimeMillis()));
		objectMetadata.setUserMetadata(imageMetadata);
		request.setMetadata(objectMetadata);
		PutObjectResult putObjectResult=s3.putObject(request);
		System.out.println("file Uploded succesfully ="+putObjectResult.getMetadata().toString());
		return s3.getUrl(bucketName, request.getKey()).toString();
	}
}
