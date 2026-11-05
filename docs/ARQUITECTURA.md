# Arquitectura y decisiones técnicas

## Flujo

Tres fuentes RSS se descargan desde Airflow en EC2. Bronze conserva los XML originales y sus metadatos. Silver limpia HTML, normaliza URL/fecha, deduplica por URL canónica y separa rechazos. Amazon Comprehend detecta idioma, entidades y frases; sus respuestas exitosas se guardan en una caché S3 por hash exacto del texto. Gold agrega noticias distintas por fecha y dimensiones. Glue registra esquemas y particiones; Athena ejecuta consultas y controles de calidad.

## Identidad e incrementalidad

- `batch_id`: identifica una extracción Bronze y sus tres originales.
- `news_id`: SHA-256 de la URL canónica.
- `content_hash`: título, descripción y fecha; detecta cambios de la noticia.
- `text_hash`: texto exacto enviado a Comprehend; evita repetir NLP cuando solo cambia otro atributo.
- `release_id`: identifica una instantánea inmutable publicada.

Los conteos Bronze representan ítems recibidos, no noticias únicas. Silver clasifica cada entrada como nueva, actualizada, sin cambio, duplicada en lote o rechazada.

## Publicación atómica

Cada etapa escribe Parquet bajo `releases/release_id=...`. Los punteros `control/silver/current.json`, `control/nlp/current.json`, `control/gold/current.json` y `control/current_release.json` se actualizan al final. Si una escritura falla antes del puntero, Athena sigue viendo la release anterior.

## Particionamiento

Silver NLP y Gold se particionan por `publication_date`. Las noticias sin fecha válida permanecen en Silver y se identifican como `unknown`, pero no se les asigna la fecha de ingesta. Glue recibe cada partición explícitamente mediante `BatchCreatePartition`.

## Limitaciones

- Comprehend identifica menciones; no resuelve alias ni garantiza que dos nombres correspondan a la misma entidad real.
- La solución reescribe instantáneas completas, adecuada para el volumen académico pero no para un lago masivo.
- La caché depende del texto exacto y de la versión de análisis; cambiar la normalización produce una nueva evaluación.
- La conservación histórica de todas las versiones de una noticia no se expone en las métricas vigentes.

