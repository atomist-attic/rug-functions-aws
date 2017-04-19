package com.atomist.rug

import com.amazonaws.auth.{AWSCredentials, AWSCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3Client}
import com.amazonaws.services.s3.model._
import com.atomist.rug.runtime.Rug
import com.atomist.rug.spi.Handlers.Status
import com.atomist.rug.spi.annotation.{Parameter, RugFunction, Secret, Tag}
import com.atomist.rug.spi.{AnnotatedRugFunction, FunctionResponse, StringBodyOption}
import com.typesafe.scalalogging.LazyLogging

import scala.collection.JavaConverters._

/**
  * TODO
  * 1. check for any truncation on list
  */
class S3BucketToBucketCopy
  extends AnnotatedRugFunction
    with Rug
    with LazyLogging {

  @RugFunction(name = "CopyS3Bucket", description = "Copy all objects from one S3 Bucket to another",
    tags = Array(new Tag(name = "s3"), new Tag(name = "copy")))
  def invoke(@Parameter(name = "region") region: String,
             @Parameter(name = "sourceBucket") sourceBucket: String,
             @Parameter(name = "destinationBucket") destinationBucket: String,
             @Secret(name = "access_key", path = "secret://team?path=aws/access_key") access_key: String,
             @Secret(name = "secret_key", path = "secret://team?path=aws/secret_key") secret_key: String): FunctionResponse = {

    try {

      def s3 = AmazonS3Client.builder().withCredentials(new AWSCredentialsProvider() {
        override def refresh(): Unit = {}

        override def getCredentials: AWSCredentials = new BasicAWSCredentials(access_key, secret_key)
      }).withRegion(region).build()

      ensureBucketExists(sourceBucket, s3 _)

      ensureBucketExists(destinationBucket, s3 _)

      val listOfS3Objects = retrieveListOfObjectsFromSourceBucket(sourceBucket, s3 _)

      listOfS3Objects.getObjectSummaries.asScala.foreach(sourceObjectSummary => {
        copySourceObjectToDestinationObject(destinationBucket, s3 _, sourceObjectSummary)
      })

      FunctionResponse(Status.Success, Some(s"Successfully copied ${listOfS3Objects.getKeyCount} keys from $sourceBucket to $destinationBucket in $region"), None, None)
    }
    catch {
      case e: Exception =>
        val msg = s"Failed to mirror $sourceBucket to $destinationBucket in $region"
        logger.warn(msg, e)
        FunctionResponse(Status.Failure, Some(msg), None, StringBodyOption(e.getMessage))
    }
  }

  private def copySourceObjectToDestinationObject(destinationBucket: String, s3: () => AmazonS3, sourceObjectSummary: S3ObjectSummary) = {
    val copyRequest = new CopyObjectRequest(sourceObjectSummary.getBucketName, sourceObjectSummary.getKey, destinationBucket, sourceObjectSummary.getKey)
    s3().copyObject(copyRequest)
  }

  private def ensureBucketExists(bucket: String, s3: () => AmazonS3) = {
    if (!s3().doesBucketExist(bucket)) {
      s3().createBucket(bucket)
    }
  }

  private def retrieveListOfObjectsFromSourceBucket(sourceBucket: String, s3: () => AmazonS3) = {
    val req = new ListObjectsV2Request().withBucketName(sourceBucket)
    s3().listObjectsV2(req)
  }
}
