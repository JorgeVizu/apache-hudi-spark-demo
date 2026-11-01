
/*
 * DEMOSTRACIÓN PRÁCTICA DE APACHE HUDI
 * Máster en Business Intelligence y Big Data
 *
 * Tecnologías:
 * - Apache Spark 3.5.7
 * - Apache Hudi 1.1.1
 * - Scala 2.12.18
 *
 * Objetivo:
 * Demostrar inserciones, UPSERT, historial de commits,
 * Time Travel y consultas incrementales.
 */

// =====================================================
// 1. CONFIGURACIÓN INICIAL
// =====================================================

// Bloque unico para que :load compile las cadenas multilinea juntas.
{
import spark.implicits._

// Ruta independiente para la demostración.
// No modificaremos la tabla original pedidos_hudi.
val rutaDemo = new java.io.File("datos/pedidos_hudi_demo").getAbsolutePath

// Nombre de la tabla Hudi.
val nombreTabla = "pedidos_hudi_demo"

println("==========================================")
println(" DEMOSTRACIÓN PRÁCTICA DE APACHE HUDI")
println("==========================================")
println("Spark: " + spark.version)
println("Tabla: " + nombreTabla)
println("Ruta: " + rutaDemo)


 
// =====================================================
// 2. CREACIÓN DE LOS DATOS INICIALES
// =====================================================

val pedidosIniciales = Seq(
  (1, "Cliente_A", 120.0, "Pendiente"),
  (2, "Cliente_B", 85.5, "Pendiente"),
  (3, "Cliente_C", 230.0, "Enviado"),
  (4, "Cliente_D", 45.0, "Pendiente"),
  (5, "Cliente_E", 310.0, "Enviado")
).toDF("id_pedido", "cliente", "importe", "estado")

println("\n--- PEDIDOS INICIALES ---")
pedidosIniciales.orderBy("id_pedido").show(false)



 
// =====================================================
// 3. ESCRITURA INICIAL EN APACHE HUDI (COPY-ON-WRITE)
// =====================================================

println("\n--- CREANDO TABLA HUDI COPY-ON-WRITE ---")

pedidosIniciales.write
  .format("hudi")
  .option("hoodie.table.name", nombreTabla)
  .option("hoodie.datasource.write.recordkey.field", "id_pedido")
  .option("hoodie.datasource.write.precombine.field", "id_pedido")
  .option("hoodie.datasource.write.operation", "insert")
  .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE")
  .mode("overwrite")
  .save(rutaDemo)

println("Tabla Hudi creada correctamente.")



 
// =====================================================
// 4. LECTURA Y VERIFICACIÓN DE LA TABLA HUDI
// =====================================================

println("\n--- LECTURA DE LA TABLA HUDI ---")

val pedidosGuardadosDemo = spark.read
  .format("hudi")
  .load(rutaDemo)

pedidosGuardadosDemo
  .select("id_pedido", "cliente", "importe", "estado")
  .orderBy("id_pedido")
  .show(false)

println("Total de pedidos almacenados: " + pedidosGuardadosDemo.count())


 
// =====================================================
// 5. UPSERT: ACTUALIZACIÓN E INSERCIÓN DE PEDIDOS
// =====================================================

println("\n--- OPERACIÓN UPSERT ---")

// Actualizamos los pedidos 1 y 2 e insertamos el pedido 6.
val pedidosActualizadosDemo = Seq(
  (1, "Cliente_A", 120.0, "Enviado"),
  (2, "Cliente_B", 85.5, "Entregado"),
  (6, "Cliente_F", 175.0, "Pendiente")
).toDF("id_pedido", "cliente", "importe", "estado")

pedidosActualizadosDemo.write
  .format("hudi")
  .option("hoodie.table.name", nombreTabla)
  .option("hoodie.datasource.write.recordkey.field", "id_pedido")
  .option("hoodie.datasource.write.precombine.field", "id_pedido")
  .option("hoodie.datasource.write.operation", "upsert")
  .mode("append")
  .save(rutaDemo)

println("UPSERT completado correctamente.")

// Volvemos a leer la tabla para obtener su estado actualizado.
val pedidosFinalesDemo = spark.read.format("hudi").load(rutaDemo)

println("\n--- TABLA DESPUÉS DEL UPSERT ---")

pedidosFinalesDemo
  .select("id_pedido", "cliente", "importe", "estado")
  .orderBy("id_pedido")
  .show(false)

println("Total de pedidos después del UPSERT: " + pedidosFinalesDemo.count())



 
// =====================================================
// 6. HISTORIAL DE COMMITS (TIMELINE)
// =====================================================

println("\n--- HISTORIAL DE COMMITS DE APACHE HUDI ---")

// Registramos la tabla en el catálogo de Spark SQL.
spark.sql(s"CREATE TABLE IF NOT EXISTS $nombreTabla USING hudi LOCATION '$rutaDemo'")

// Consultamos las operaciones registradas por Hudi.
spark.sql(s"CALL show_commits(table => '$nombreTabla', limit => 10)")
  .select(
    "commit_time",
    "action",
    "total_records_written",
    "total_update_records_written",
    "total_errors"
  )
  .show(false)

// Recuperamos los identificadores de los commits.
val commitsDemo = spark.sql(s"CALL show_commits(table => '$nombreTabla', limit => 10)")
  .select("commit_time")
  .collect()
  .map(_.getString(0))
  .sorted

val primerCommitDemo = commitsDemo.head
val segundoCommitDemo = commitsDemo.last

println("Primer commit: " + primerCommitDemo)
println("Segundo commit: " + segundoCommitDemo)



 
// =====================================================
// 7. TIME TRAVEL: CONSULTAR UNA VERSIÓN ANTERIOR
// =====================================================

println("\n--- TIME TRAVEL: ESTADO ORIGINAL DE LA TABLA ---")

// Recuperamos el estado de la tabla en el primer commit.
val historicoDemo = spark.read
  .format("hudi")
  .option("as.of.instant", primerCommitDemo)
  .load(rutaDemo)

// Mostramos los pedidos tal como estaban antes del UPSERT.
historicoDemo
  .select("id_pedido", "cliente", "importe", "estado")
  .orderBy("id_pedido")
  .show(false)

println("Commit consultado: " + primerCommitDemo)
println("Registros recuperados mediante Time Travel: " + historicoDemo.count())



 
// =====================================================
// 8. CONSULTA INCREMENTAL
// =====================================================

println("\n--- LECTURA INCREMENTAL DE APACHE HUDI ---")

// Consultamos los registros asociados al segundo commit.
val cambiosIncrementalesDemo = spark.read
  .format("hudi")
  .option("hoodie.datasource.query.type", "incremental")
  .option("hoodie.datasource.read.begin.instanttime", segundoCommitDemo)
  .load(rutaDemo)

// Mostramos los registros recuperados.
cambiosIncrementalesDemo
  .select("id_pedido", "cliente", "estado", "_hoodie_commit_time")
  .orderBy("id_pedido")
  .show(false)

println("Registros recuperados mediante lectura incremental: " +
  cambiosIncrementalesDemo.count())

// Verificamos que Spark utiliza el lector incremental de Hudi.
println("\n--- PLAN DE EJECUCIÓN INCREMENTAL ---")
cambiosIncrementalesDemo.explain(true)



 
// =====================================================
// 9. VERIFICACIÓN FINAL DE LA DEMOSTRACIÓN
// =====================================================

println("\n==========================================")
println(" VERIFICACIÓN FINAL DE APACHE HUDI")
println("==========================================")

val totalInicial = historicoDemo.count()
val totalActual = pedidosFinalesDemo.count()
val totalIncremental = cambiosIncrementalesDemo.count()

println("Registros iniciales: " + totalInicial)
println("Registros actuales: " + totalActual)
println("Registros del segundo commit: " + totalIncremental)

// Comprobamos que los resultados coinciden con lo esperado.
require(totalInicial == 5, "Error: se esperaban 5 registros iniciales.")
require(totalActual == 6, "Error: se esperaban 6 registros actuales.")
require(totalIncremental == 3, "Error: se esperaban 3 registros incrementales.")
require(commitsDemo.length == 2, "Error: se esperaban exactamente 2 commits.")

// Verificamos las actualizaciones y la nueva inserción.
val estadosEsperados = Map(
  1 -> "Enviado",
  2 -> "Entregado",
  6 -> "Pendiente"
)

val estadosReales = pedidosFinalesDemo
  .select("id_pedido", "estado")
  .collect()
  .map(fila => fila.getInt(0) -> fila.getString(1))
  .toMap

estadosEsperados.foreach { case (id, estado) =>
  require(
    estadosReales.get(id).contains(estado),
    s"Error: el pedido $id no tiene el estado esperado: $estado"
  )
}

println("\nTODAS LAS VERIFICACIONES SUPERADAS")
println("DEMOSTRACIÓN DE APACHE HUDI COMPLETADA")
println("==========================================")
}
