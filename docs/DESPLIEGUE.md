# Despliegue reproducible

## 1. Recursos AWS

Usar la región `us-east-2` y un solo bucket S3 privado. Mantener bloqueado todo acceso público y habilitar cifrado SSE-S3. Crear una EC2 Ubuntu `t3.small` con 30 GiB gp3, sin exponer Airflow a Internet y con SSH limitado a la IP del estudiante.

Adjuntar a la EC2 un rol de instancia con permisos mínimos sobre:

- el bucket del laboratorio y sus objetos;
- Amazon Comprehend: detección de idioma, entidades y frases clave;
- Glue Data Catalog: base de datos, tablas y particiones;
- Athena: consultas, workgroup y metadatos de Glue.

No crear usuarios IAM ni guardar llaves AWS en `.env`. El SDK obtiene credenciales temporales desde el rol EC2.

## 2. Preparación de EC2

Instalar Git, Docker Engine y el complemento Docker Compose. Clonar el repositorio y crear la configuración local:

```bash
git clone https://github.com/jnicortes1611/data-lake-rss-aws.git
cd data-lake-rss-aws
cp .env.example .env
```

Editar solamente el nombre del bucket en `.env`:

```text
DATA_LAKE_BUCKET=nombre-del-bucket
AWS_DEFAULT_REGION=us-east-2
AIRFLOW_UID=50000
```

Construir y arrancar los servicios:

```bash
sudo docker compose up -d --build
sudo docker compose ps
sudo docker compose logs airflow | tail -n 50
```

La primera ejecución de `airflow standalone` muestra en los logs las credenciales iniciales del administrador. Cambiarlas después de ingresar.

## 3. Acceso seguro

Desde el equipo local abrir un túnel SSH, sustituyendo la llave y la IP:

```powershell
ssh -i "RUTA_LLAVE.pem" -N -L 127.0.0.1:8080:127.0.0.1:8080 ubuntu@IP_EC2
```

Abrir `http://127.0.0.1:8080`. El puerto 8080 queda enlazado únicamente a localhost en EC2.

## 4. Ejecución

Activar `rss_pipeline`. Para una ingesta nueva usar:

```json
{"mode":"ingest","batch_id":"not-used","max_records":1000}
```

Para reprocesar exclusivamente desde Bronze:

```json
{"mode":"reprocess","batch_id":"rss-IDENTIFICADOR","max_records":1000}
```

El flujo publica primero objetos inmutables y actualiza los punteros `control/*/current.json` únicamente después de validar cada release. Consultar los indicadores con `sql/consultas-athena.sql`.

## 5. Verificación

```bash
sudo docker compose exec airflow python -B -m unittest discover -s /opt/airflow/tests -v
sudo docker compose logs --tail=100 airflow
```

En AWS verificar el manifiesto Bronze, los punteros Silver/NLP/Gold, las siete tablas Glue, las particiones y los resultados del workgroup `rss_lab`.

## 6. Pausa y recuperación

```bash
sudo docker compose stop
```

Después detener la EC2. Para continuar, iniciar la instancia y ejecutar `sudo docker compose up -d`. Los datos permanecen en S3 y PostgreSQL permanece en el volumen Docker.

## 7. Eliminación definitiva

Antes de eliminar, conservar las evidencias requeridas. Confirmar que los recursos no sean compartidos y luego:

1. ejecutar `sudo docker compose down -v`;
2. vaciar y eliminar el bucket S3;
3. eliminar las tablas y la base `rss_lake` de Glue;
4. eliminar el workgroup y resultados de Athena;
5. eliminar la instancia, el volumen EBS y el grupo de seguridad;
6. eliminar el rol y sus políticas si fueron creados solo para el taller;
7. revisar Cost Explorer para confirmar que no queden recursos facturables.
