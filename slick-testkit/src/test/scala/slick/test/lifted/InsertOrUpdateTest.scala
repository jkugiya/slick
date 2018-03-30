package slick.test.lifted

import com.typesafe.slick.testkit.util.StandardTestDBs._
import com.typesafe.slick.testkit.util.{DBTest, DBTestObject, JdbcTestDB}
import org.junit.{Before, Test}

import scala.concurrent.Await
import scala.concurrent.duration._

object InsertOrUpdateTest extends DBTestObject(
  H2Mem, SQLiteMem, Postgres, MySQL, DerbyMem, HsqldbMem, SQLServerJTDS, SQLServerSQLJDBC
)

class InsertOrUpdateTest(val tdb: JdbcTestDB) extends DBTest {

  import tdb.profile.api._

  class T(tag: Tag) extends Table[Int](tag, "mytable") {
    def id = column[Int]("id", O.PrimaryKey, O.AutoInc)

    def * = id
  }

  case class V(id: String, value: String)

  class T2(tag: Tag) extends Table[V](tag, "mytable2") {
    def id = column[String]("id", O.PrimaryKey)

    def value = column[String]("value")

    def * = (id, value) <> ((V.apply _).tupled, V.unapply)
  }

  case class V2(i1: Int, i2: Int, v: String)

  class T3(tag: Tag) extends Table[V2](tag, "mytable3") {
    def i1 = column[Int]("i1")

    def i2 = column[Int]("i2")

    def v = column[String]("v")

    def pk = primaryKey("pk_t3", (i1, i2))

    def * = (i1, i2, v) <> ((V2.apply _).tupled, V2.unapply)
  }

  def t1: TableQuery[T] = TableQuery[T]

  def t2: TableQuery[T2] = TableQuery[T2]

  def t3: TableQuery[T3] = TableQuery[T3]

  implicit class Run[R, E <: Effect](dbio: DBIOAction[R, NoStream, E]) {
    def run(): R = Await.result(db.run(dbio), Duration.Inf)
  }

  @Before def before(): Unit = {
    t1.schema.create.run
    t2.schema.create.run
    t3.schema.create.run
  }

  // Sigle column
  @Test def ignore_if_specified_same_value_on_single_column: Unit = {
    // Given
    (t1 += 1).run
    // When
    t1.insertOrUpdate(1).run
    // Then
    (t1.filter(_ === 1).length.result.run == 1)
  }

  @Test def insert_another_record_if_different_value_specified: Unit = {
    // Given
    (t1 += 1).run
    // When
    t1.insertOrUpdate(2).run
    // Then
    (t1.filter(_ === 1).length.result.run == 1)
    (t1.filter(_ === 2).length.result.run == 1)
  }

  // Single PK with value
  @Test def ignore_if_specified_all_same_value_on_multiple_column: Unit = {
    // Given
    (t2 += V("1", "v")).run
    // When
    t2.insertOrUpdate(V("1", "v")).run
    // Then
    (t2.filter(_.id === "1").result.head.run == V("1", "v"))
    (t2.filter(_.id === "1").length.result.run == 1)
  }

  @Test def update_value_if_different_value_specified_on_same_primary_key: Unit = {
    // Given
    (t2 += V("1", "v")).run
    // When
    t2.insertOrUpdate(V("1", "v2")).run
    // Then
    (t2.filter(_.id === "1").result.head.run == V("1", "v2"))
    (t2.filter(_.id === "1").length.result.run == 1)
  }

  @Test def insert_if_different_primary_key_specified: Unit = {
    // Given
    (t2 += V("1", "v")).run
    // When
    t2.insertOrUpdate(V("2", "v")).run
    // Then
    (t2.filter(_.id === "1").result.head.run == V("1", "v"))
    (t2.filter(_.id === "2").result.head.run == V("1", "v"))
    (t2.length.result.run == 2)
  }

  // Composite PK with value
  @Test def ignore_if_specified_all_same_value_on_multiple_column_with_composite_pk: Unit = {
    // Given
    (t3 += V2(1, 1, "v")).run
    // When
    t3.insertOrUpdate(V2(1, 1, "v")).run
    // Then
    (t3.filter(_.i1 === 1).result.head.run == V2(1, 1, "v"))
    (t3.filter(_.i1 === 1).length.result.run == 1)
  }

  @Test def update_value_if_different_value_specified_on_same_composite_pk: Unit = {
    // Given
    (t3 += V2(1, 1, "v")).run
    // When
    t3.insertOrUpdate(V2(1, 1, "v2")).run
    // Then
    (t3.filter(_.i1 === 1).result.head.run == V2(1, 1, "v2"))
    (t3.filter(_.i1 === 1).length.result.run == 1)
  }

  @Test def insert_if_different_composite_pk_specified: Unit = {
    // Given
    (t3 += V2(1, 1, "v")).run
    // When
    t3.insertOrUpdate(V2(1, 2, "v")).run
    // Then
    (t3.filter(_.i2 === 1).result.head.run == V2(1, 1, "v"))
    (t3.filter(_.i2 === 2).result.head.run == V2(1, 2, "v"))
    (t3.length.result.run == 2)
  }
}

