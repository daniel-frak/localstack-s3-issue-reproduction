# Issue

The problem is described in issue [#10988](https://github.com/localstack/localstack/issues/10988) on the LocalStack Github page.

# Running the sample

Run the test in the [`LocalStackIssueReproductionTest.java`](https://github.com/daniel-frak/localstack-s3-issue-reproduction/blob/main/src/test/java/com/example/localstacks3downloadissuerepro/LocalStackIssueReproductionTest.java) class.

The test should fail with:
```
org.apache.http.ConnectionClosedException: Premature end of Content-Length delimited message body (expected: 10,000,000; received: 9,502,720)
```
