package com.atomist.rug

import com.atomist.rug.runtime.RugSupport
import com.atomist.rug.spi.Handlers.Status
import com.atomist.rug.spi.annotation.{Parameter, RugFunction, Secret, Tag}
import com.atomist.rug.spi.{AnnotatedRugFunction, FunctionResponse, JsonBodyOption, StringBodyOption}
import com.typesafe.scalalogging.LazyLogging

class MirrorRepoToS3
  extends AnnotatedRugFunction
    with RugSupport
    with LazyLogging{

  @RugFunction(name = "MirrorRepoToS3", description = "Fancy new rug function",
    tags = Array(new Tag(name = "fancy"), new Tag(name = "rug")))
  def invoke(@Parameter(name = "somenum") number: Int,
             @Parameter(name = "somestr") repo: String,
             @Secret(name = "super_secret", path = "user/system/token?scope=theworld") token: String): FunctionResponse = {

    try {
      FunctionResponse(Status.Success, Option("Successfully ran function.."), None, JsonBodyOption(None))
    } catch {
      case e: Exception => FunctionResponse(Status.Failure, Some("Error running function..."), None, StringBodyOption(e.getMessage))
    }
  }
}
