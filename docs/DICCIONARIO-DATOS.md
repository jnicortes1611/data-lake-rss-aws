# Diccionario de datos

## Silver

### news

Una fila por noticia vigente. Clave: `news_id`.

| Campo | Tipo | Descripción |
|---|---|---|
| news_id | string | SHA-256 de la URL canónica |
| canonical_url | string | URL normalizada |
| title | string | Título limpio |
| description | string | Descripción limpia |
| pub_date_utc | string | Fecha ISO-8601 UTC |
| content_hash | string | Hash de contenido relevante |
| version | bigint | Versión vigente incremental |
| medio | string | Medio principal |
| section_primary | string | Sección principal |
| publication_date | partition string | Fecha UTC o `unknown` |

### news_sources

Una fila por asociación noticia/fuente. Contiene `news_id`, `source_id`, `medio`, `seccion`, lotes y marcas de primera/última observación.

### news_entities

Una fila por entidad normalizada distinta en cada noticia. Clave lógica: `news_id`, `entity_type`, `entity_normalized`.

Campos principales: `entity_text`, `entity_normalized`, `entity_type`, `confidence`, `language`, `text_hash`, `analyzed_at_utc`, `publication_date`.

### news_key_phrases

Una fila por frase normalizada distinta en cada noticia. Clave lógica: `news_id`, `phrase_normalized`.

Campos principales: `phrase_text`, `phrase_normalized`, `confidence`, `language`, `text_hash`, `analyzed_at_utc`, `publication_date`.

### quarantine

Una fila por registro rechazado, con lote, fuente, índice, campos originales, motivo y fecha de cuarentena. Permite trazabilidad hasta el XML Bronze.

## Gold

| Tabla | Granularidad | Métrica |
|---|---|---|
| news_daily | fecha, medio, sección | noticias distintas |
| entity_daily | fecha, medio, tipo, entidad normalizada | noticias distintas que mencionan la entidad |
| key_phrase_section_daily | fecha, medio, sección, frase normalizada | noticias distintas que contienen la frase |

Una noticia puede contribuir a varias secciones mediante `news_sources`; por eso la suma por secciones puede superar el total único del medio.

