// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.base.s3;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandler;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * Simple standalone S3 client for uploading, downloading and listing objects from S3.
 */
@SuppressWarnings("JavadocLinkAsPlainText")
public class S3Client implements AutoCloseable {
    /* Set the system property to allow restricted headers in HttpClient */
    static {
        System.setProperty("jdk.httpclient.allowRestrictedHeaders", "Host,Content-Length");
    }
    /** HMAC SHA256 algorithm for signing **/
    private static final String ALGORITHM_HMAC_SHA256 = "HmacSHA256";
    /** SHA256 hash of an empty request body **/
    private static final String EMPTY_BODY_SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    /** Unsigned payload header value **/
    private static final String UNSIGNED_PAYLOAD = "UNSIGNED-PAYLOAD";
    /** AWS4 signature scheme and algorithm **/
    private static final String SCHEME = "AWS4";
    /** AWS4 signature algorithm **/
    private static final String ALGORITHM = "HMAC-SHA256";
    /** AWS4 signature terminator **/
    private static final String TERMINATOR = "aws4_request";
    /** Format strings for the date/time and date stamps required during signing **/
    private static final DateTimeFormatter DATE_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(java.time.ZoneOffset.UTC);
    /** Format string for the date stamp **/
    private static final DateTimeFormatter DATE_STAMP_FORMATTER =
            DateTimeFormatter.ofPattern("yyyyMMdd").withZone(java.time.ZoneOffset.UTC);
    /** The query parameter separator **/
    private static final char QUERY_PARAMETER_SEPARATOR = '&';
    /** The query parameter value separator **/
    private static final char QUERY_PARAMETER_VALUE_SEPARATOR = '=';

    /** The S3 region name **/
    private final String regionName;
    /** The S3 endpoint URL **/
    private final String endpoint;
    /** The S3 bucket name **/
    private final String bucketName;
    /** The S3 access key **/
    private final String accessKey;
    /** The S3 secret key **/
    private final String secretKey;
    /** The HTTP client used for making requests **/
    private final HttpClient httpClient;

    /**
     * Constructor for S3Client.
     *
     * @param endpoint The S3 endpoint URL (e.g. "https://s3.amazonaws.com/").
     * @param bucketName The name of the S3 bucket.
     * @param accessKey The S3 access key.
     * @param secretKey The S3 secret key.
     */
    public S3Client(
            final String regionName,
            final String endpoint,
            final String bucketName,
            final String accessKey,
            final String secretKey) {
        this.regionName = regionName;
        this.endpoint = endpoint.endsWith("/") ? endpoint : endpoint + "/";
        this.bucketName = bucketName;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(30))
                .build();
    }

    /**
     * Closes the HTTP client.
     *
     * @throws Exception if an error occurs while closing the client
     */
    @Override
    public void close() throws Exception {
        this.httpClient.close();
    }

    /**
     * Lists objects in the S3 bucket with the specified prefix
     *
     * @param prefix The prefix to filter the objects.
     * @param maxResults The maximum number of results to return.
     * @return A list of object keys.
     */
    public List<String> listObjects(String prefix, int maxResults) {
        String canonicalQueryString = "list-type=2&prefix=" + prefix + "&max-keys=" + maxResults;
        HttpResponse<Document> response =
                requestXML(endpoint + bucketName + "/?" + canonicalQueryString, "GET", Collections.emptyMap(), null);
        // extract the object keys from the XML response
        List<String> keys = new ArrayList<>();
        // Get all "Contents" elements
        NodeList contentsNodes = response.body().getElementsByTagName("Contents");
        for (int i = 0; i < contentsNodes.getLength(); i++) {
            Element contentsElement = (Element) contentsNodes.item(i);

            // Get the "Key" element inside each "Contents"
            NodeList keyNodes = contentsElement.getElementsByTagName("Key");
            if (keyNodes.getLength() > 0) {
                keys.add(keyNodes.item(0).getTextContent());
            }
        }
        return keys;
    }

    /**
     * Uploads a file to S3 using multipart upload, assumes the file is small enough as uses single part upload.
     *
     * @param objectKey the key for the object in S3 (e.g., "myfolder/myfile.txt")
     * @param storageClass the storage class (e.g., "STANDARD", "REDUCED_REDUNDANCY")
     * @param content the content of the file as a string
     * @return true if the upload was successful, false otherwise
     */
    public boolean uploadTextFile(String objectKey, String storageClass, String content) {
        final byte[] contentData = content.getBytes(StandardCharsets.UTF_8);
        final Map<String, String> headers = new HashMap<>();
        headers.put("content-length", Integer.toString(contentData.length));
        headers.put("content-type", "text/plain");
        headers.put("x-amz-storage-class", storageClass);
        headers.put("x-amz-content-sha256", base64(sha256(contentData)));
        HttpResponse<Document> response =
                requestXML(endpoint + bucketName + "/" + urlEncode(objectKey, true), "PUT", headers, contentData);
        if (response.statusCode() != 200) {
            throw new RuntimeException("Failed to upload text file: " + response.statusCode() + "\n" + response.body());
        }
        return true;
    }

    /**
     * Downloads a text file from S3, assumes the file is small enough as uses single part download.
     *
     * @param objectKey the key for the object in S3 (e.g., "myfolder/myfile.txt")
     * @return the content of the file as a string, null if the file doesn't exist
     */
    public String downloadTextFile(String objectKey) {
        final Map<String, String> headers = new HashMap<>();
        HttpResponse<String> response = request(
                endpoint + bucketName + "/" + urlEncode(objectKey, true),
                "GET",
                headers,
                new byte[0],
                BodyHandlers.ofString(StandardCharsets.UTF_8));
        if (response.statusCode() == 404) {
            return null;
        } else if (response.statusCode() != 200) {
            throw new RuntimeException(
                    "Failed to download text file: " + response.statusCode() + "\n" + response.body());
        }
        return response.body();
    }

    /**
     * Uploads a file to S3 using multipart upload
     *
     * @param objectKey the key for the object in S3 (e.g., "myfolder/myfile.txt")
     * @param contentIterable an Iterable of byte arrays representing the file content
     * @param contentType the content type of the file (e.g., "text/plain")
     * @return true if the upload was successful, false otherwise
     */
    public boolean uploadFile(
            String objectKey, String storageClass, Iterator<byte[]> contentIterable, String contentType) {
        // start the multipart upload
        final String uploadId = createMultipartUpload(objectKey, storageClass, contentType);
        // create a list to store the ETags of the uploaded parts
        final List<String> eTags = new ArrayList<>();
        // Multipart upload works in chunks of 5MB
        final byte[] chunk = new byte[5 * 1024 * 1024];
        // track the offset in the chunk
        int offsetInChunk = 0;
        // We need to iterate of the contentIterable and build 5MB chunks that we can upload
        // to S3. We will use System.arrayCopy() to build the chunks.
        while (contentIterable.hasNext()) {
            final byte[] next = contentIterable.next();
            int remainingContent = next.length;
            while (remainingContent > 0) {
                // if the remaining content is larger than the chunk size, we need to split it
                if (remainingContent > chunk.length - offsetInChunk) {
                    int length = chunk.length - offsetInChunk;
                    System.arraycopy(next, next.length - remainingContent, chunk, offsetInChunk, length);
                    remainingContent -= length;
                    // we now have a full chunk so need to upload the chunk to S3
                    eTags.add(multipartUploadPart(objectKey, uploadId, eTags.size() + 1, chunk));
                    // reset the offset in the chunk
                    offsetInChunk = 0;
                } else {
                    // we have a partial chunk so need to copy the remaining content to the chunk
                    System.arraycopy(next, next.length - remainingContent, chunk, offsetInChunk, remainingContent);
                    offsetInChunk += remainingContent;
                    remainingContent = 0;
                }
            }
        }
        // now upload the last chunk if it is not empty
        if (offsetInChunk > 0) {
            // extract just the content of the chunk
            byte[] lastChunk = new byte[offsetInChunk];
            System.arraycopy(chunk, 0, lastChunk, 0, offsetInChunk);
            // we have a partial chunk so need to upload it to S3
            eTags.add(multipartUploadPart(objectKey, uploadId, eTags.size() + 1, lastChunk));
        }
        // Complete the multipart upload
        completeMultipartUpload(objectKey, uploadId, eTags);
        return true;
    }

    /**
     * Creates a multipart upload for the specified object key.
     *
     * @param key The object key.
     * @param storageClass The storage class (e.g. "STANDARD", "REDUCED_REDUNDANCY").
     * @param contentType The content type of the object.
     * @return The upload ID for the multipart upload.
     */
    String createMultipartUpload(String key, String storageClass, String contentType) {
        String canonicalQueryString = "uploads=";
        Map<String, String> headers = new HashMap<>();
        headers.put("content-type", contentType);
        if (storageClass != null) {
            headers.put("x-amz-storage-class", storageClass);
        }
        // TODO add checksum algorithm and overall checksum support using x-amz-checksum-algorithm=SHA256 and
        //  x-amz-checksum-type=COMPOSITE
        try {
            HttpResponse<Document> response =
                    requestXML(endpoint + bucketName + "/" + key + "?" + canonicalQueryString, "POST", headers, null);
            if (response.statusCode() != 200) {
                throw new RuntimeException("Failed to create multipart upload: " + response.statusCode());
            }
            return response.body().getElementsByTagName("UploadId").item(0).getTextContent();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Uploads a part of a multipart upload.
     *
     * @param key The object key.
     * @param uploadId The upload ID for the multipart upload.
     * @param partNumber The part number (1-based).
     * @param partData The data for the part.
     * @return The ETag of the uploaded part.
     */
    String multipartUploadPart(String key, String uploadId, int partNumber, byte[] partData) {
        String canonicalQueryString = "uploadId=" + uploadId + "&partNumber=" + partNumber;
        Map<String, String> headers = new HashMap<>();
        headers.put("content-length", Integer.toString(partData.length));
        headers.put("content-type", "application/octet-stream");
        headers.put("x-amz-content-sha256", base64(sha256(partData)));
        try {
            HttpResponse<Document> response = requestXML(
                    endpoint + bucketName + "/" + key + "?" + canonicalQueryString, "PUT", headers, partData);
            if (response.statusCode() != 200) {
                throw new RuntimeException("Failed to upload multipart part: " + response.statusCode());
            }
            return response.headers().firstValue("ETag").orElse(null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Completes a multipart upload.
     *
     * @param key The object key.
     * @param uploadId The upload ID for the multipart upload.
     * @param eTags The list of ETags for the uploaded parts.
     */
    void completeMultipartUpload(String key, String uploadId, List<String> eTags) {
        String canonicalQueryString = "uploadId=" + uploadId;
        Map<String, String> headers = new HashMap<>();
        headers.put("content-type", "application/xml");
        StringBuilder sb = new StringBuilder();
        sb.append("<CompleteMultipartUpload>");
        for (int i = 0; i < eTags.size(); i++) {
            sb.append("<Part><PartNumber>")
                    .append(i + 1)
                    .append("</PartNumber><ETag>")
                    .append(eTags.get(i))
                    .append("</ETag></Part>");
        }
        sb.append("</CompleteMultipartUpload>");
        byte[] partData = sb.toString().getBytes(StandardCharsets.UTF_8);
        HttpResponse<Document> response =
                requestXML(endpoint + bucketName + "/" + key + "?" + canonicalQueryString, "POST", headers, partData);
        if (response.statusCode() != 200) {
            throw new RuntimeException("Failed to complete multipart upload: " + response.statusCode());
        }
    }

    /**
     * Performs an HTTP request to S3 to the specified URL with the given parameters.
     *
     * @param url         The URL to send the request to.
     * @param httpMethod  The HTTP method to use (e.g. "GET", "POST", "PUT").
     * @param headers     The request headers to send.
     * @param requestBody The request body to send, or null if no request body is needed.
     * @return HTTP response and result parsed as an XML document. If there is no response body, the result is empty
     * document.
     */
    private HttpResponse<Document> requestXML(
            final String url, final String httpMethod, final Map<String, String> headers, byte[] requestBody) {
        return request(url, httpMethod, headers, requestBody, new XmlBodyHandler());
    }

    /**
     * Performs an HTTP request to S3 to the specified URL with the given parameters.
     *
     * @param url         The URL to send the request to.
     * @param httpMethod  The HTTP method to use (e.g. "GET", "POST", "PUT").
     * @param headers     The request headers to send.
     * @param requestBody The request body to send, or null if no request body is needed.
     * @param bodyHandler The body handler for parsing response.
     * @return HTTP response and result parsed using the provided body handler.
     */
    private <T> HttpResponse<T> request(
            final String url,
            final String httpMethod,
            final Map<String, String> headers,
            byte[] requestBody,
            BodyHandler<T> bodyHandler) {
        try {
            // the region-specific endpoint to the target object expressed in path style
            URI endpointUrl = new URI(url);

            Map<String, String> h = new HashMap<>(headers);
            final String contentHashString;
            if (requestBody == null || requestBody.length == 0) {
                contentHashString = EMPTY_BODY_SHA256;
                requestBody = new byte[0];
            } else {
                contentHashString = UNSIGNED_PAYLOAD;
                h.put("content-length", "" + requestBody.length);
            }
            h.put("x-amz-content-sha256", contentHashString);

            Map<String, String> q = extractQueryParameters(endpointUrl);
            String authorization = computeSignatureForAuthorizationHeader(
                    endpointUrl, httpMethod, regionName, h, q, contentHashString, accessKey, secretKey);

            // place the computed signature into a formatted 'Authorization' header and call S3
            h.put("Authorization", authorization);
            // build the request
            HttpRequest.Builder requestBuilder = HttpRequest.newBuilder(endpointUrl);
            requestBuilder = switch (httpMethod) {
                case "POST" -> requestBuilder.POST(HttpRequest.BodyPublishers.ofByteArray(requestBody));
                case "PUT" -> requestBuilder.PUT(HttpRequest.BodyPublishers.ofByteArray(requestBody));
                case "GET" -> requestBuilder.GET();
                default -> throw new IllegalArgumentException("Unsupported HTTP method: " + httpMethod);};
            requestBuilder = requestBuilder.headers(h.entrySet().stream()
                    .flatMap(entry -> Stream.of(entry.getKey(), entry.getValue()))
                    .toArray(String[]::new));

            HttpRequest request = requestBuilder.build();

            return httpClient.send(request, bodyHandler);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException | URISyntaxException e) {
            throw new UncheckedIOException(new IOException(e));
        }
    }

    /**
     * Extract parameters from a query string, preserving encoding.
     *
     * @param endpointUrl The endpoint URL to extract parameters from.
     * @return The list of parameters, in the order they were found.
     */
    private static Map<String, String> extractQueryParameters(URI endpointUrl) {
        final String rawQuery = endpointUrl.getQuery();
        if (rawQuery == null) {
            return Collections.emptyMap();
        } else {
            Map<String, String> results = new HashMap<>();
            int endIndex = rawQuery.length() - 1;
            int index = 0;
            while (index <= endIndex) {
                // Ideally we should first look for '&', then look for '=' before the '&', but that's not how AWS
                // understand query parsing. A string such as "?foo&bar=qux" will be understood as one parameter with
                // the
                // name "foo&bar" and value "qux".
                String name;
                String value;
                int nameValueSeparatorIndex = rawQuery.indexOf(QUERY_PARAMETER_VALUE_SEPARATOR, index);
                if (nameValueSeparatorIndex < 0) {
                    // No value
                    name = rawQuery.substring(index);
                    value = null;

                    index = endIndex + 1;
                } else {
                    int parameterSeparatorIndex = rawQuery.indexOf(QUERY_PARAMETER_SEPARATOR, nameValueSeparatorIndex);
                    if (parameterSeparatorIndex < 0) {
                        parameterSeparatorIndex = endIndex + 1;
                    }
                    name = rawQuery.substring(index, nameValueSeparatorIndex);
                    value = rawQuery.substring(nameValueSeparatorIndex + 1, parameterSeparatorIndex);

                    index = parameterSeparatorIndex + 1;
                }
                // note that value = null is valid as we can have a parameter without a value in
                // a query string (legal http)
                results.put(
                        URLDecoder.decode(name, StandardCharsets.UTF_8),
                        value == null ? value : URLDecoder.decode(value, StandardCharsets.UTF_8));
            }
            return results;
        }
    }

    /**
     * Computes an AWS4 signature for a request, ready for inclusion as an 'Authorization' header.
     *
     * @param endpointUrl     the url to which the request is being made
     * @param httpMethod      the HTTP method (GET, POST, PUT, etc.)
     * @param regionName      the AWS region name
     * @param headers         The request headers; 'Host' and 'X-Amz-Date' will be added to this set.
     * @param queryParameters Any query parameters that will be added to the endpoint. The parameters should be
     *                        specified in canonical format.
     * @param bodyHash        Precomputed SHA256 hash of the request body content; this value should also be set as the
     *                        header 'X-Amz-Content-SHA256' for non-streaming uploads.
     * @param awsAccessKey    The user's AWS Access Key.
     * @param awsSecretKey    The user's AWS Secret Key.
     * @return The computed authorization string for the request. This value needs to be set as the header
     * 'Authorization' on the further HTTP request.
     */
    private static String computeSignatureForAuthorizationHeader(
            final URI endpointUrl,
            final String httpMethod,
            final String regionName,
            final @NonNull Map<String, String> headers,
            final @NonNull Map<String, String> queryParameters,
            final String bodyHash,
            final String awsAccessKey,
            final String awsSecretKey) {
        // first, get the date and time for the further request, and convert
        // to ISO 8601 format for use in signature generation
        final ZonedDateTime now = ZonedDateTime.now(java.time.ZoneOffset.UTC);
        final String dateTimeStamp = DATE_TIME_FORMATTER.format(now);
        final String dateStamp = DATE_STAMP_FORMATTER.format(now);

        // update the headers with required 'x-amz-date' and 'host' values
        headers.put("x-amz-date", dateTimeStamp);

        String hostHeader = endpointUrl.getHost();
        final int port = endpointUrl.getPort();
        if (port > -1) {
            hostHeader = hostHeader.concat(":" + port);
        }
        headers.put("Host", hostHeader);

        // canonicalize the headers; we need the set of header names as well as the
        // names and values to go into the signature process
        final String canonicalizedHeaderNames = headers.keySet().stream()
                .map(header -> header.toLowerCase(Locale.ENGLISH))
                .sorted(String.CASE_INSENSITIVE_ORDER)
                .collect(Collectors.joining(";"));
        // The canonical header requires value entries in sorted order, and multiple white spaces in the values should
        // be compressed to a single space.
        final String canonicalizedHeaders = headers.entrySet().stream()
                        .sorted(Entry.comparingByKey(String.CASE_INSENSITIVE_ORDER))
                        .map(entry -> entry.getKey().toLowerCase(Locale.ENGLISH).replaceAll("\\s+", " ") + ":"
                                + entry.getValue().replaceAll("\\s+", " "))
                        .collect(Collectors.joining("\n"))
                + "\n";

        // if any query string parameters have been supplied, canonicalize them
        final String canonicalizedQueryParameters = queryParameters.entrySet().stream()
                .map(entry -> urlEncode(entry.getKey(), false) + "="
                        + (entry.getValue() == null ? "" : urlEncode(entry.getValue(), false)))
                .sorted()
                .collect(Collectors.joining("&"));

        // canonicalizedResourcePath
        final String path = endpointUrl.getPath();
        final String canonicalizedResourcePath = path.isEmpty() ? "/" : urlEncode(path, true);
        // canonicalize the various components of the request
        final String canonicalRequest = httpMethod + "\n"
                + canonicalizedResourcePath + "\n"
                + canonicalizedQueryParameters + "\n"
                + canonicalizedHeaders + "\n"
                + canonicalizedHeaderNames + "\n"
                + bodyHash;

        // construct the string to be signed
        final String scope = dateStamp + "/" + regionName + "/" + "s3" + "/" + TERMINATOR;
        final String stringToSign = SCHEME + "-" + ALGORITHM + "\n" + dateTimeStamp + "\n" + scope + "\n"
                + HexFormat.of().formatHex(sha256(canonicalRequest.getBytes(StandardCharsets.UTF_8)));

        // compute the signing key
        final byte[] kSecret = (SCHEME + awsSecretKey).getBytes(StandardCharsets.UTF_8);
        final byte[] kDate = sign(dateStamp, kSecret);
        final byte[] kRegion = sign(regionName, kDate);
        final byte[] kService = sign("s3", kRegion);
        final byte[] kSigning = sign(TERMINATOR, kService);
        final byte[] signature = sign(stringToSign, kSigning);

        final String credentialsAuthorizationHeader = "Credential=" + awsAccessKey + "/" + scope;
        final String signedHeadersAuthorizationHeader = "SignedHeaders=" + canonicalizedHeaderNames;
        final String signatureAuthorizationHeader =
                "Signature=" + HexFormat.of().formatHex(signature);

        return SCHEME + "-" + ALGORITHM + " " + credentialsAuthorizationHeader + ", " + signedHeadersAuthorizationHeader
                + ", " + signatureAuthorizationHeader;
    }

    /**
     * Signs the given data using HMAC SHA256 with the specified key.
     *
     * @param stringData The data to sign.
     * @param key The key to use for signing.
     * @return The signed data as a byte array.
     */
    private static byte[] sign(String stringData, byte[] key) {
        try {
            Mac mac = Mac.getInstance(S3Client.ALGORITHM_HMAC_SHA256);
            mac.init(new SecretKeySpec(key, S3Client.ALGORITHM_HMAC_SHA256));
            return mac.doFinal(stringData.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Encodes the given URL using UTF-8 encoding.
     *
     * @param url the URL to encode
     * @param keepPathSlash true, if slashes in the path should be preserved, false
     * @return the encoded URL
     */
    private static String urlEncode(String url, boolean keepPathSlash) {
        String encoded;
        encoded = URLEncoder.encode(url, StandardCharsets.UTF_8).replace("+", "%20");
        if (keepPathSlash) {
            return encoded.replace("%2F", "/");
        } else {
            return encoded;
        }
    }

    /**
     * Computes the SHA-256 hash of the given data.
     *
     * @param data the data to hash
     * @return the SHA-256 hash as a byte array
     */
    private static byte[] sha256(byte[] data) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update(data);
            return md.digest();
        } catch (NoSuchAlgorithmException e) {
            throw new UncheckedIOException(new IOException(e));
        }
    }

    /**
     * Encodes the byte array to a base64 string.
     *
     * @param data the byte array to encode
     * @return the base64 encoded string
     */
    private static String base64(byte[] data) {
        return new String(Base64.getEncoder().encode(data));
    }
}
