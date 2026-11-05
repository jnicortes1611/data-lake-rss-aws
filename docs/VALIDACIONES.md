# Validaciones de calidad, incrementalidad y recuperación

## Escenarios demostrados

| Escenario | Evidencia/resultado |
|---|---|
| Carga inicial | Bronze, Silver y Gold publicados y consultables en Athena |
| Registro defectuoso | Lote controlado envió un registro a quarantine con motivo y batch_id |
| Repetición exitosa | Silver, NLP y Gold reutilizaron releases sin duplicar |
| Lote incremental real | 30 entradas: 2 actualizadas y 28 sin cambio |
| Lote controlado | 32 entradas: 1 nueva, 2 actualizadas, 28 sin cambio y 1 rechazada |
| Texto modificado | 79 noticias: 77 cache hits, 2 misses y 6 llamadas Comprehend |
| Fallo controlado | Lote inexistente produjo tarea roja y auditoría `FAILED` |
| Recuperación | Reejecución con lote válido terminó completa sin duplicar |
| Reproceso Bronze | `mode=reprocess` reutilizó XML existentes sin HTTP |
| Nueva partición | `publication_date=2026-11-04` registrada en Glue y consultada en Athena |

## Controles automáticos

`rss_catalog`/`rss_pipeline` comprueban mediante Athena:

- unicidad de `news_id`;
- campos obligatorios;
- formato de enlace y fecha;
- ausencia de entidades huérfanas;
- ausencia de frases huérfanas;
- tablas Gold no vacías.

La consulta de conciliación en `sql/consultas-athena.sql` debe devolver cero filas. La comparación con y sin filtro de partición mostró menos filas y bytes de entrada al filtrar `publication_date`.

## Lote controlado reproducible

`rss_prepare_validation_batch` copia un lote Bronze completo, modifica el texto de una noticia conservando su URL, agrega una URL nueva y añade un ítem inválido. Nunca cambia el lote base ni consulta internet. Luego `rss_pipeline` lo procesa con `mode=reprocess`.

## Orquestación 4.6

El DAG integral tiene programación horaria, dependencias explícitas, reintentos acotados, `max_active_runs=1`, parámetros de reproceso y auditorías finales. Los DAG individuales se conservan para diagnóstico, pero la demostración usa `rss_pipeline`.

