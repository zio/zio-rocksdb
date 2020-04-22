package zio.rocksdb.internal

package internal

import java.io.IOException
import java.nio.file.{ Files, Path }

import zio.{ Task, UIO, ZIO, ZManaged }

import scala.reflect.io.Directory

object ManagedPath {
  private def createTempDirectory: Task[Path] = Task {
    Files.createTempDirectory("zio-rocksdb")
  }

  private def deleteDirectory(path: Path): UIO[Boolean] = UIO {
    new Directory(path.toFile).deleteRecursively()
  }

  private def deleteDirectoryE(path: Path): UIO[Unit] =
    deleteDirectory(path) >>= {
      case true  => ZIO.unit
      case false => ZIO.die(new IOException("Could not delete path recursively"))
    }

  def apply(): ZManaged[Any, Throwable, Path] = createTempDirectory.toManaged(deleteDirectoryE)
}
