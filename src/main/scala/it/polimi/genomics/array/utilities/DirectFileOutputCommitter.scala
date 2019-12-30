package it.polimi.genomics.array.utilities

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.output.{FileOutputCommitter, FileOutputFormat}
import org.apache.hadoop.mapreduce.{JobContext, TaskAttemptContext}


/**
  * Created by Olga Gorlova on 15/11/2019.
  */
class DirectFileOutputCommitter (
                                  outputPath: Path,
                                  context: JobContext
                                ) extends FileOutputCommitter(outputPath, context) {

  override def isRecoverySupported: Boolean = true

  override def getCommittedTaskPath(context: TaskAttemptContext): Path = getWorkPath

  override def getCommittedTaskPath(appAttemptId: Int, context: TaskAttemptContext): Path = getWorkPath

  override def getJobAttemptPath(context: JobContext): Path = getWorkPath

  override def getJobAttemptPath(appAttemptId: Int): Path = getWorkPath

  override def needsTaskCommit(taskAttemptContext: TaskAttemptContext): Boolean = true

  override def getWorkPath: Path = outputPath

  override def commitTask(context: TaskAttemptContext, taskAttemptPath: Path): Unit = {
    if(outputPath != null) {
      context.progress()
    }
  }

  override def commitJob(jobContext: JobContext): Unit = {

    val conf = jobContext.getConfiguration
    val shouldCreateSuccessFile = jobContext
      .getConfiguration
      .getBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs", true)

    if (shouldCreateSuccessFile) {
      val outputPath = FileOutputFormat.getOutputPath(jobContext)
      if (outputPath != null) {
        val fileSys = outputPath.getFileSystem(conf)
        val filePath = new Path(outputPath, FileOutputCommitter.SUCCEEDED_FILE_NAME)
        fileSys.create(filePath).close()
      }
    }
  }
}
