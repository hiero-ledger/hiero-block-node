package org.hiero.block.node.archive;

import java.io.IOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Base64;

/**
 * This class provides a method to upload files to Amazon S3 using the HTTP PUT method.
 */
public class S3Upload {
    /** The logger for this class. */
    private static final Logger LOGGER = System.getLogger(S3Upload.class.getName());

    /**
     * Uploads a file to S3 using the HTTP PUT method.
     *
     * @param endpoint the S3 endpoint (e.g., "https://s3.amazonaws.com/")
     * @param bucketName the name of the S3 bucket
     * @param objectKey the key for the object in S3 (e.g., "myfolder/myfile.txt")
     * @param accessKey the access key for S3
     * @param secretKey the secret key for S3
     * @param contentIterable an Iterable of byte arrays representing the file content
     * @param contentType the content type of the file (e.g., "text/plain")
     * @return true if the upload was successful, false otherwise
     */
    public static boolean uploadFile(String endpoint, String bucketName, String objectKey,
            String accessKey, String secretKey, Iterable<byte[]> contentIterable, String contentType) {
        // Create HttpClient
        try (var client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(30))
                .build()) {

            // Determine content type
            contentType = (contentType == null) ? "application/octet-stream" : contentType;

            // Create URI for S3 object
            var uri = URI.create(endpoint + bucketName + objectKey);

            // Basic auth header (note: real S3 typically uses AWS Signature v4)
            var auth = "Basic " + Base64.getEncoder().encodeToString((accessKey + ":" + secretKey).getBytes());

            // Create body publisher from file (streams the file)
            var bodyPublisher = HttpRequest.BodyPublishers.ofByteArrays(contentIterable);

            // Build request
            var request = HttpRequest.newBuilder()
                    .uri(uri)
                    .PUT(bodyPublisher)
                    .header("Content-Type", contentType)
//                    .header("Content-Length", "0")
                    .header("Authorization", auth)
                    .build();

            // Send request and get response
            var response = client.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200 || response.statusCode() == 201) {
                System.out.println("File uploaded successfully");
                return true;
            } else {
                System.out.println("Upload failed with status code: " + response.statusCode());
                System.out.println("Response: " + response.body());
            }
        } catch (IOException | InterruptedException e) {
            LOGGER.log(Level.ERROR, "Error uploading file: " + e.getMessage(), e);
        }
        return false;
    }
}
