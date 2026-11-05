# Índice de evidencias

Guardar las capturas fuera del repositorio si contienen identificadores de cuenta. No incluir contraseñas, MFA, llaves PEM ni `.env`.

## Arquitectura medallón

- Bronze: DAG verde, XML y metadatos de tres fuentes.
- Silver: métricas, Parquet, quarantine y puntero current.
- NLP: DAG verde, caché S3, 77 hits/2 misses y tablas normalizadas.
- Gold: DAG verde, tres conjuntos agregados y release publicada.
- Glue: siete tablas y partición `2026-11-04`.
- Athena: cinco consultas, conciliación sin diferencias y comparación de bytes.

## Sección 4.5

- `evidencia-4-5-lote-incremental-real.png`
- `evidencia-4-5-registro-defectuoso-quarantine.png`
- `evidencia-4-5-cache-solo-textos-cambiados.png`
- `evidencia-4-5-fallo-controlado-auditoria.png`
- `evidencia-4-5-fallo-controlado-error-lote-inexistente.png`
- `evidencia-4-5-recuperacion-rejecucion-exitosa.png`
- `evidencia-4-5-nueva-particion-athena.png`
- `evidencia-4-5-bytes-con-y-sin-filtro.png`

## Sección 4.6

- `evidencia-4-6-grafo-dag-integral.png`
- `evidencia-4-6-ejecucion-completa-verde.png`

