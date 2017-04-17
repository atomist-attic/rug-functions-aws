package com.atomist.rug

import java.io.File
import java.nio.file.FileSystems

import com.amazonaws.auth.{AWSCredentials, AWSCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.{BucketVersioningConfiguration, ObjectMetadata, PutObjectRequest, SetBucketVersioningConfigurationRequest}
import com.atomist.rug.runtime.Rug
import com.atomist.rug.spi.Handlers.Status
import com.atomist.rug.spi.annotation.{Parameter, RugFunction, Secret, Tag}
import com.atomist.rug.spi.{AnnotatedRugFunction, FunctionResponse, StringBodyOption}
import com.atomist.source.SimpleCloudRepoId
import com.atomist.source.filter.ArtifactFilter
import com.atomist.source.github.{GitHubArtifactSource, GitHubArtifactSourceLocator, GitHubServices}
import com.typesafe.scalalogging.LazyLogging

import scala.util.{Success, Try}


/**
  * TODO
  * 1. content types
  * 2. path manipulation. Insert prefix path. Remove root path from file path
  * 3. Add glob support
  */
class MirrorRepoToS3
  extends AnnotatedRugFunction
    with Rug
    with LazyLogging{

  @RugFunction(name = "MirrorRepoToS3", description = "Mirror a GitHub repo to s3",
    tags = Array(new Tag(name = "s3"), new Tag(name = "mirror")))
  def invoke(@Parameter(name = "owner") owner: String,
             @Parameter(name = "repo") repo: String,
             @Parameter(name = "region") region: String,
             @Parameter(name = "bucket") bucket: String,
             @Parameter(name = "glob") glob: String,
             @Secret(name = "user_token", path = "github://user_token?scopes=repo") token: String,
             @Secret(name = "access_key", path = "secret://team?path=aws/access_key") access_key: String,
             @Secret(name = "secret_key", path = "secret://team?path=aws/secret_key") secret_key: String): FunctionResponse = {


    try {
      def creds  = new BasicAWSCredentials(access_key, secret_key)
      def s3 = AmazonS3Client.builder().withCredentials(new AWSCredentialsProvider(){
        override def refresh(): Unit = {}
        override def getCredentials: AWSCredentials = creds
      }).withRegion(region).build()

      // Ensure bucket
      if (!s3.doesBucketExist(bucket)) {
        s3.createBucket(bucket)
      }

      // Enable versioning
      def configRequest =
        new SetBucketVersioningConfigurationRequest(bucket,
          new BucketVersioningConfiguration(BucketVersioningConfiguration.ENABLED))
      s3.setBucketVersioningConfiguration(configRequest)

      // Fetch file list TODO - add glob
      val cri = SimpleCloudRepoId(repo, owner)
      val tghas = GitHubArtifactSource(GitHubArtifactSourceLocator(cri), GitHubServices(token), new ArtifactFilter {
        override def apply(s: String) = {
          val fs = FileSystems.getDefault
          Try(fs.getPathMatcher(s"glob:$glob")) match {
            case Success(matcher) => matcher.matches(new File(s).toPath)
            case _ => false
          }
        }
      })

      tghas.allFiles.foreach(file => {
        def meta = new ObjectMetadata()
        meta.setContentLength(file.contentLength)
        def request = new PutObjectRequest(bucket, file.path, file.inputStream(), meta)
        s3.putObject(request)
      })
      FunctionResponse(Status.Success, Some(s"Successfully mirrored ${tghas.allFiles.size} files from $owner/$repo to $bucket in $region"), None, None)
    }
    catch {
      case e: Exception =>
        val msg = s"Failed to mirror $owner/$repo to $bucket in $region"
        logger.warn(msg, e)
        FunctionResponse(Status.Failure, Some(msg), None, StringBodyOption(e.getMessage))
    }
  }
}
