package com.atomist.rug

import java.io.FileInputStream
import java.util.Properties

import org.scalatest.{FlatSpec, Matchers}

/**
  * To execute this test:
  *  - Ensure you have replaced the AWS credentials with the valid IAM user's credentials that you want to use
  *  - Ensure the source and destination buckets exist, and that there are some objects in the source bucket to validate against
  *
  * Todo:
  *  - Should really validate the contents are actually copied between the buckets, but this is really a utility function at the moment for local test purposes.
  */
class S3BucketToBucketCopyIntegrationTest extends FlatSpec with Matchers {

  val region = "US_EAST"

  val sourceBucket = "fpimagesrv-staging"

  val destinationBucket = "fpimagesrv-production"

  val uut = new S3BucketToBucketCopy

  val AWS_KEY_PROPERTY_NAME = "AWS_KEY"
  val AWS_SECRET_PROPERTY_NAME = "AWS_SECRET"

  // Change "ignore" to "it" to turn this test on for local verification
  ignore should "copy from existing source bucket to destination bucket" in {

    val prop = new Properties()
    prop.load(new FileInputStream("src/test/resources/aws.properties"))

    val awsKey = prop.get(AWS_KEY_PROPERTY_NAME).asInstanceOf[String]
    val awsSecret = prop.get(AWS_SECRET_PROPERTY_NAME).asInstanceOf[String]

    uut.invoke(region, sourceBucket, destinationBucket, awsKey, awsSecret)
  }
}
