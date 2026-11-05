# Índice de evidencias

Las capturas están organizadas por capa y por requisito del taller. Los archivos originales se conservaron sin cambios; estas son copias con nombres descriptivos.

> Nota de privacidad: algunas capturas muestran el identificador de la cuenta, el nombre del usuario de AWS, el nombre del bucket y el identificador canónico del propietario. No contienen contraseñas, llaves de acceso, MFA ni archivos PEM. El repositorio debe mantenerse privado hasta redactar esos datos.

## 1. Bronze

- `01-bronze/dag-rss-bronze-tareas-exitosas.jpeg`: ejecución exitosa del DAG Bronze.
- `01-bronze/s3-tres-fuentes-bronze.jpeg`: estructura S3 con las tres fuentes RSS.
- `01-bronze/bronze-manifest-lote-completo.png`: manifiesto resumido con lote, estado, fuentes, conteos, hashes y rutas.

## 2. Silver

- `02-silver/dag-rss-silver-tareas-exitosas.jpeg`: ejecución exitosa del DAG Silver.
- `02-silver/metricas-transformacion-inicial.jpeg`: métricas iniciales de transformación.
- `02-silver/release-silver-publicada.jpeg`: versión Silver inmutable publicada.
- `02-silver/parquet-tabla-noticias.jpeg`: archivo Parquet de noticias.
- `02-silver/parquet-relacion-fuentes-secciones.jpeg`: archivo de relación entre noticias, fuentes y secciones.
- `02-silver/puntero-version-vigente.jpeg`: puntero `control/silver/current.json`.
- `02-silver/objeto-registros-rechazados.jpeg`: objeto Parquet en cuarentena.
- `02-silver/silver-registro-rechazado-con-motivo.png`: rechazo con `batch_id`, fuente, motivo y datos originales.

## 3. Amazon Comprehend y caché persistente

- `03-comprehend/dag-rss-comprehend-tareas-exitosas.jpeg`: DAG de enriquecimiento exitoso.
- `03-comprehend/log-enriquecimiento-comprehend-exitoso.jpeg`: métricas de la primera ejecución.
- `03-comprehend/primera-ejecucion-cache-misses.jpeg`: llamadas iniciales y fallos de caché esperados.
- `03-comprehend/segunda-ejecucion-reused-release.jpeg`: reutilización de la versión.
- `03-comprehend/cache-persistente-objetos-s3.jpeg`: objetos de caché persistidos en S3.
- `03-comprehend/s3-release-news-nlp-parquet.jpeg`: publicación de `news_nlp.parquet`.
- `03-comprehend/puntero-nlp-current-json.jpeg`: puntero de la versión NLP vigente.

## 4. Gold

- `04-gold/dag-rss-gold-tareas-exitosas.jpeg`: tareas construir y verificar exitosas.
- `04-gold/verificacion-gold-metricas.jpeg`: estado `VERIFIED` y métricas de agregación.
- `04-gold/segunda-ejecucion-idempotente.jpeg`: segunda ejecución con `reused_release: True`.
- `04-gold/s3-release-news-daily.jpeg`: agregado diario de noticias.
- `04-gold/s3-release-entity-daily.jpeg`: agregado diario de entidades.
- `04-gold/s3-release-key-phrase-section-daily.jpeg`: agregado de frases clave por sección y día.
- `04-gold/puntero-gold-current-json.jpeg`: puntero Gold vigente.

## 5. AWS Glue y Amazon Athena

- `05-glue-athena/glue-data-catalog-tablas.jpeg`: tablas registradas en el catálogo.
- `05-glue-athena/glue-siete-tablas-release-final.png`: siete tablas del release final obtenidas desde Glue mediante el rol EC2.
- `05-glue-athena/schema-news-daily-particionado.jpeg`: esquema y clave de partición de `news_daily`.
- `05-glue-athena/control-catalogo-current-json.jpeg`: control del catálogo.
- `05-glue-athena/control-release-vigente-current-json.jpeg`: versión publicada vigente.
- `05-glue-athena/resultados-athena-en-s3.jpeg`: resultados de Athena almacenados en S3.
- `05-glue-athena/consulta-noticias-fecha-seccion.jpeg`: noticias por fecha y sección.
- `05-glue-athena/consulta-top-diez-organizaciones.jpeg`: diez organizaciones presentes en más noticias.
- `05-glue-athena/consulta-comparacion-entidad-entre-dias.jpeg`: comparación de una entidad entre días.
- `05-glue-athena/consulta-frases-clave-por-seccion.jpeg`: frases clave más frecuentes por sección.
- `05-glue-athena/consulta-conciliacion-gold-silver.jpeg`: conciliación Gold contra Silver sin diferencias.
- `05-glue-athena/consulta-tabla-gold.jpeg`: consulta adicional sobre una tabla Gold.

## 6. Validaciones de la sección 4.5

- `06-validaciones-4-5/silver-lote-incremental-metricas.jpeg`: lote incremental con nuevos, actualizados y cuarentena.
- `06-validaciones-4-5/quarantine-lote-validacion-rejected-parquet.jpeg`: registro defectuoso enviado a cuarentena.
- `06-validaciones-4-5/comprehend-solo-textos-cambiados.jpeg`: 77 aciertos, 2 fallos de caché y 6 llamadas a Comprehend.
- `06-validaciones-4-5/bronze-reproceso-idempotente-reused-true.jpeg`: reproceso Bronze reutilizado.
- `06-validaciones-4-5/fallo-controlado-auditoria.png`: auditoría ejecutada ante un fallo controlado.
- `06-validaciones-4-5/fallo-controlado-lote-inexistente.png`: error esperado por lote inexistente.
- `06-validaciones-4-5/recuperacion-reejecucion-exitosa.png`: recuperación posterior exitosa.
- `06-validaciones-4-5/glue-nueva-particion-2026-11-04.jpeg`: nueva partición visible en Glue.
- `06-validaciones-4-5/athena-consulta-con-filtro-particion.jpeg`: consulta con filtro.
- `06-validaciones-4-5/athena-consulta-sin-filtro-particion.jpeg`: consulta equivalente sin filtro.
- `06-validaciones-4-5/athena-estadisticas-con-filtro.jpeg`: 31 filas y 4.33 KB leídos.
- `06-validaciones-4-5/athena-estadisticas-sin-filtro.jpeg`: 79 filas y 10.70 KB leídos.

## 7. Orquestación integral de la sección 4.6

- `07-orquestacion-4-6/grafo-dag-integral.png`: dependencias del pipeline completo.
- `07-orquestacion-4-6/pipeline-integral-tareas-verdes.jpeg`: ejecución completa con tareas exitosas.
- `07-orquestacion-4-6/pipeline-integral-ejecucion-exitosa.jpeg`: vista complementaria de la ejecución integral.

## Cobertura

Las evidencias incluyen los escenarios funcionales, de calidad, incrementalidad, idempotencia, particionamiento, reproceso y recuperación solicitados para el Data Lake.

