package main;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;

public class test {

    public static void main(String[] args) throws FileNotFoundException {
        String bucketName = "deneme";
        String serviceEndpoint = "https://172.16.7.155:8081";

        if (args.length == 0) {
            System.out.println("lütfen dosya pathini giriniz");
            System.exit(0);
        }
        if(args[0].equals("-h")) {
            System.out.println("Örnek kullanım :");
            System.out.println("java -jar app.jar file/path/file.extension  http://[ip]:[port] [bucketName]");
            System.exit(1);
        }
        String path = args[0];
        if(args.length>1) {
            serviceEndpoint = args[1];
        }
        if(args.length>2) {
            bucketName = args[2];
        }


        File f = null;
        try {
            f = new File(path);
        } catch (Exception e) {
            System.out.println("bu path te dosya bulunamadı : " + path);
            System.exit(1);
        }

        AmazonS3Client s3 = new AmazonS3Client(bucketName, serviceEndpoint);
        s3.download("unknown");

        String testId = "burak";
        s3.put(testId, new FileInputStream(f), (int) f.length());
        s3.delete(testId);

        String multiPartTestId = "multipartBurak";
        String uploadId = s3.initializeMultiPartUpload(multiPartTestId);
        String eTag = s3.uploadPart(multiPartTestId, uploadId, 1, new FileInputStream(f), (int) f.length());
        Map<Integer, String> parts = new HashMap<>();
        parts.put(1,eTag);
        s3.completeMultipartUpload(multiPartTestId, uploadId, parts);
        s3.download(multiPartTestId);

    }
}
