-- Sustituir SUFIJO por el sufijo de las tablas de control/catalog/current.json.
-- Release validada en el laboratorio: daf5f274a9a2.

-- 1. Noticias por fecha y sección.
SELECT DISTINCT n.publication_date, s.seccion, s.medio, n.news_id, n.title, n.pub_date_utc
FROM news_daf5f274a9a2 n
JOIN news_sources_daf5f274a9a2 s
  ON n.news_id = s.news_id AND n.publication_date = s.publication_date
WHERE n.publication_date = '2026-11-04'
ORDER BY s.seccion, n.pub_date_utc DESC;

-- 2. Diez organizaciones presentes en más noticias.
SELECT entity_normalized, count(DISTINCT news_id) AS noticias
FROM news_entities_daf5f274a9a2
WHERE entity_type = 'ORGANIZATION'
GROUP BY entity_normalized
ORDER BY noticias DESC, entity_normalized
LIMIT 10;

-- 3. Presencia de una entidad entre días. Reemplazar el valor indicado.
SELECT publication_date, count(DISTINCT news_id) AS noticias
FROM news_entities_daf5f274a9a2
WHERE entity_type = 'ORGANIZATION'
  AND entity_normalized = '<ENTIDAD_NORMALIZADA>'
GROUP BY publication_date
ORDER BY publication_date;

-- 4. Frases clave más frecuentes por sección.
SELECT s.seccion, p.phrase_normalized, count(DISTINCT p.news_id) AS noticias
FROM news_key_phrases_daf5f274a9a2 p
JOIN news_sources_daf5f274a9a2 s
  ON p.news_id = s.news_id AND p.publication_date = s.publication_date
GROUP BY s.seccion, p.phrase_normalized
ORDER BY s.seccion, noticias DESC, p.phrase_normalized;

-- 5. Conciliación Gold contra Silver. Resultado esperado: cero filas.
WITH expected AS (
  SELECT n.publication_date, s.medio, s.seccion, count(DISTINCT n.news_id) AS news_count
  FROM news_daf5f274a9a2 n
  JOIN news_sources_daf5f274a9a2 s
    ON n.news_id = s.news_id AND n.publication_date = s.publication_date
  WHERE n.publication_date <> 'unknown'
  GROUP BY 1, 2, 3
), actual AS (
  SELECT publication_date, medio, seccion, news_count
  FROM news_daily_daf5f274a9a2
)
SELECT coalesce(e.publication_date, g.publication_date) AS publication_date,
       coalesce(e.medio, g.medio) AS medio,
       coalesce(e.seccion, g.seccion) AS seccion,
       e.news_count AS silver_count, g.news_count AS gold_count
FROM expected e
FULL OUTER JOIN actual g
  ON e.publication_date = g.publication_date
 AND e.medio = g.medio AND e.seccion = g.seccion
WHERE coalesce(e.news_count, -1) <> coalesce(g.news_count, -1)
ORDER BY 1, 2, 3;

-- 6a/6b. Comparación de bytes escaneados con y sin filtro de partición.
SELECT sum(length(title)) FROM news_daf5f274a9a2 WHERE publication_date = '2026-11-04';
SELECT sum(length(title)) FROM news_daf5f274a9a2;
