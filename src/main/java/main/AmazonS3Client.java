package main;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class AmazonS3Client {

    private String bucketName;
    private AmazonS3 s3Client;

    public AmazonS3Client(String bucketName, String serviceEndpoint) {
        this.bucketName = bucketName;
        s3Client = AmazonS3ClientBuilder
                .standard()
                .withCredentials(new AWSCredentialsProvider() {
                    @Override
                    public AWSCredentials getCredentials() {
                        return new AnonymousAWSCredentials();
                    }

                    @Override
                    public void refresh() {

                    }
                })
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(serviceEndpoint, Regions.US_EAST_1.name()))
                .withPathStyleAccessEnabled(true)
                .build();
    }

    public void delete(String objectId) {
        DeleteObjectRequest request = new DeleteObjectRequest(bucketName, objectId);
        try {
            s3Client.deleteObject(request);
        } catch (Exception e) {
            System.out.println("delete error : ");
            e.printStackTrace();
        }
    }

    public void put(String objectId, InputStream inputStream, int length) {
        try {
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(length);
            PutObjectRequest request = new PutObjectRequest(bucketName, objectId, inputStream, metadata);
            request.setMetadata(metadata);
            PutObjectResult result = s3Client.putObject(request);
            System.out.println("file saved with eTag : " + result.getETag());
        } catch (Exception e) {
            System.out.println("put fail");
            e.printStackTrace();
            throw new RuntimeException("download - aws sdk exception: ", e);
        }
    }

    public void download(String objectId) {
        S3Object object;
        GetObjectRequest request = new GetObjectRequest(bucketName, objectId);

        try {
            object = s3Client.getObject(request);
            System.out.println("download success objectId : " + objectId + ", size : " + object.getObjectMetadata().getContentLength());
        } catch (Exception e) {
            System.out.println("download fail");
            e.printStackTrace();
            throw new RuntimeException("download - aws sdk exception: ", e);
        }

    }

    public String initializeMultiPartUpload(String objectId) {
        InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(bucketName, objectId);
        InitiateMultipartUploadResult initResponse = s3Client.initiateMultipartUpload(initRequest);
        try {
            String uploadId = initResponse.getUploadId();
            System.out.println("initializeMultiPartUpload done, returns : " + uploadId);
            return uploadId;
        } catch (Exception e) {
            System.out.println("initializeMultiPartUpload fail");
            e.printStackTrace();
            throw new RuntimeException("initializeMultiPartUpload - aws sdk exception");
        }
    }

    public String uploadPart(String objectId, String uploadId, int index, InputStream content, int length) {
        UploadPartRequest uploadRequest = new UploadPartRequest().
                withBucketName(bucketName).withKey(objectId).
                withUploadId(uploadId).withPartNumber(index).
                withFileOffset(0).withInputStream(content).
                withPartSize(length);
        try {
            String eTag = s3Client.uploadPart(uploadRequest).getETag();
            System.out.println("uploadPart success, returns : " + eTag);
            return eTag;
        } catch (Exception e) {
            System.out.println("uploadPart fail");
            e.printStackTrace();
            throw new RuntimeException("uploadPart - aws sdk exception");
        }
    }

    public void completeMultipartUpload(String objectId, String uploadId, Map<Integer, String> parts) {
        List<PartETag> eTags = new ArrayList<>();
        parts.forEach((partIndex, partTag) -> eTags.add(new PartETag(partIndex, partTag)));
        CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest(bucketName, objectId, uploadId, eTags);
        try {
            s3Client.completeMultipartUpload(compRequest);
            System.out.println("completeMultipartUpload success");
        } catch (Exception e) {
            System.out.println("completeMultipartUpload fail");
            e.printStackTrace();
            throw new RuntimeException("completeMultipartUpload - aws sdk exception");
        }
    }
}


