# Estimación de costos

Estimación orientativa para un laboratorio pequeño en `us-east-2`; los precios reales deben verificarse en AWS Pricing Calculator y Cost Explorer antes de desplegar.

## Supuestos mensuales

- EC2 `t3.small`: 20 horas encendida.
- EBS gp3: 30 GiB durante todo el mes.
- S3 Standard: menos de 1 GiB y algunos miles de solicitudes.
- Comprehend: 100 noticias nuevas, tres operaciones por texto.
- Glue: menos de un millón de objetos y solicitudes; sin crawlers ni jobs ETL.
- Athena: menos de 1 GiB escaneado gracias a Parquet y particiones.
- Una IPv4 pública solo mientras EC2 está encendida.

## Fórmula y orden de magnitud

| Componente | Fórmula de trabajo | Estimación |
|---|---|---:|
| EC2 | 20 h × ~USD 0,021/h | ~USD 0,42 |
| EBS gp3 | 30 GiB × ~USD 0,08/GiB-mes | ~USD 2,40 |
| S3 | almacenamiento + PUT/GET/LIST | < USD 0,10 |
| Comprehend | unidades de 100 caracteres, mínimo 3 por operación | normalmente cubierto por nivel gratuito inicial; < USD 1 para este volumen |
| Glue Data Catalog | objetos y accesos | USD 0 dentro del primer millón |
| Athena | bytes escaneados × tarifa por TB | centavos o menos |
| IPv4 pública | horas de uso; puede aplicar franquicia o tarifa | verificar en factura |

Total orientativo sin créditos: aproximadamente **USD 3 a 5 por mes** con 20 horas de EC2. Mantener EC2 encendida todo el mes elevaría principalmente EC2 e IPv4. EBS y S3 continúan facturándose aunque la instancia esté detenida.

## Controles

- Presupuesto configurado en USD 10.
- Detener Docker y EC2 al terminar cada sesión.
- Caché persistente para evitar repetir Comprehend.
- Parquet y particiones para reducir bytes Athena.
- Límite de 10 MiB por consulta en el workgroup `rss_lab`.
- Sin crawlers ni jobs Glue facturables.

Referencias: páginas oficiales de precios de EC2, EBS, S3, Comprehend, Glue, Athena y VPC. Fecha de revisión: 4 de noviembre de 2026.

