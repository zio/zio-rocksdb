package zio.rocksdb.internal

import zio.{ Scope, Task, URIO, ZIO }

import java.io.File
import java.nio.file.{ Files, Path }

object ManagedPath {
  private def createTempDirectory: Task[Path] = ZIO.attempt {
    Files.createTempDirectory("zio-rocksdb")
  }

  private def deleteDirectory(path: Path): Task[Unit] =
    ZIO.attempt {
      def deleteRecursively(file: File): Unit = {
        if (file.isDirectory) {
          file.listFiles.foreach(deleteRecursively)
        }
        if (file.exists && !file.delete) {
          throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
        }

        deleteRecursively(path.toFile)
      }
    }

  private def deleteDirectoryE(path: Path): URIO[Any, Unit] =
    deleteDirectory(path).orDie

  def apply(): ZIO[Scope, Throwable, Path] = ZIO.acquireRelease(createTempDirectory)(deleteDirectoryE)
}
