# Validacion local de Bronze

Última ejecución: 4 de noviembre de 2026.

Comando: `python -B -m unittest discover -s tests -v`

Resultado observado: 20 pruebas, todas correctas (OK).

Escenarios cubiertos:

1. AccessDenied se propaga y no se interpreta como objeto inexistente.
2. Corrupcion del XML detectada por hash.
3. Fallo entre XML y metadata.json; reintento sin nueva descarga.
4. Carga inicial conserva los bytes originales y concilia conteos.
5. Cuerpo vacio, XML malformado, HTML y DTD son rechazados.
6. Reproceso con original faltante informa error sin descargarlo.
7. Una nueva ejecucion genera otro lote de extraccion.
8. Una ingesta parcial reintenta solo la fuente faltante.
9. Repetir la ejecucion mantiene identificador, rutas y originales.
10. Reprocesar el batch_id original desde otra ejecucion no consulta HTTP.
11. Reprocesar lote desconocido no crea una extraccion nueva.
12. RSS valido sin items se distingue de una fuente fallida.
13. Limpieza, URL canónica y fechas UTC.
14. Deduplicación entre fuentes y cuarentena.
15. Actualización de versión ante cambios.
16. Reutilización de una release Silver sin duplicar.
17. Un fallo antes de publicar no reemplaza el puntero vigente.
18. El texto NLP usa título, salto de línea y descripción.
19. La clave de caché cambia únicamente cuando cambia el texto.
20. Gold cuenta noticias distintas aunque una entidad o frase se repita.

El almacenamiento S3 y las respuestas HTTP son simulados. No se ha comprobado la disponibilidad real de RSS, IAM, red de EC2, importacion del DAG en Airflow ni escritura en S3. Eso se verificara y documentara por separado.

La traza ServiceUnavailable que aparece durante una prueba es una falla inyectada deliberadamente; la prueba confirma su recuperacion.
