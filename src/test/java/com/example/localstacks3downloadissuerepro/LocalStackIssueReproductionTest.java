package com.example.localstacks3downloadissuerepro;


import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class LocalStackIssueReproductionTest {

    @Container
    private static final LocalStackContainer LOCAL_STACK_CONTAINER = new LocalStackContainer(
            DockerImageName.parse("localstack/localstack:3.4.0"));

    private static final String BUCKET_NAME = "test-bucket";
    private static final String OBJECT_KEY = "test-file";
    private static final int LARGE_FILE_SIZE_IN_BYTES = 10_000_000;
    private static final int SLEEP_TIME_MILLIS = 90_000;

    private S3Client s3Client;

    @BeforeEach
    void setUp(@TempDir Path tempDir) throws IOException {
        s3Client = buildS3Client();
        uploadLargeFileToS3(tempDir);
    }

    private S3Client buildS3Client() {
        return S3Client.builder()
                .endpointOverride(LOCAL_STACK_CONTAINER.getEndpointOverride(LocalStackContainer.Service.S3))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
                        LOCAL_STACK_CONTAINER.getAccessKey(), LOCAL_STACK_CONTAINER.getSecretKey())))
                .region(Region.of(LOCAL_STACK_CONTAINER.getRegion()))
                .build();
    }

    private void uploadLargeFileToS3(Path tempDir) throws IOException {
        File file = createLargeFileLocally(tempDir);
        uploadFileToS3(file);
    }

    private File createLargeFileLocally(Path tempDir) throws IOException {
        Path filePath = tempDir.resolve("testFile.txt");
        byte[] content = new byte[LARGE_FILE_SIZE_IN_BYTES];
        Files.write(filePath, content, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        return filePath.toFile();
    }

    private void uploadFileToS3(File file) {
        s3Client.createBucket(request -> request.bucket(BUCKET_NAME));
        s3Client.putObject(request -> request.bucket(BUCKET_NAME).key(OBJECT_KEY),
                RequestBody.fromFile(file));
    }

    @Test
    void shouldImportLargeFileOverLongPeriod() {
        List<String> fileContents = readFileContentsFromS3();
        assertThatFileSizeIsCorrect(fileContents);
    }

    @SuppressWarnings("java:S2925") // Intentionally using Thread.sleep() to induce issue
    @SneakyThrows
    private List<String> readFileContentsFromS3() {
        ResponseInputStream<GetObjectResponse> response = s3Client.getObject(request ->
                request.bucket(BUCKET_NAME).key(OBJECT_KEY));

        List<String> fileContents = new ArrayList<>();
        try (var reader = new BufferedReader(new InputStreamReader(response, StandardCharsets.UTF_8))) {
            Thread.sleep(SLEEP_TIME_MILLIS);
            String line;
            while ((line = reader.readLine()) != null) {
                fileContents.add(line);
            }
        }
        return fileContents;
    }

    private void assertThatFileSizeIsCorrect(List<String> fileContents) {
        int sizeInBytes = fileContents.stream()
                .mapToInt(String::length)
                .sum();
        assertThat(sizeInBytes).isEqualTo(LARGE_FILE_SIZE_IN_BYTES);
    }
}
