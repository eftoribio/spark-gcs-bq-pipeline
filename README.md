# Creating a data pipeline with PySpark, Google Cloud Storage, and BigQuery

To follow along, fork this repository and create a GitHub codespace. You can also run this locally provided you have Python and Docker installed.

## 0. Setting up the environment

Create the `docker-compose.yaml` file
```yaml
services:
  spark:
    build: .
    environment:
      - SPARK_MODE=master
    ports:
      - '8080:8080'
      - '4040:4040'
    volumes:
      - ./data:/data
      - ./src:/src
  spark-worker:
    build: .
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark:7077
      - SPARK_WORKER_MEMORY=4G
      - SPARK_EXECUTOR_MEMORY=4G
      - SPARK_WORKER_CORES=4
    volumes:
      - ./data:/data
      - ./src:/src 
```
Set up the Dockerfile. We're using [Bitnami package for Spark](https://bitnami.com/stack/spark/containers)
```Dockerfile
FROM docker.io/bitnami/spark:3.5.1

COPY *.jar $SPARK_HOME/jars

RUN mkdir -p $SPARK_HOME/secrets
COPY ./src/credentials/gcp-credentials.json $SPARK_HOME/secrets/gcp-credentials.json
ENV GOOGLE_APPLICATION_CREDENTIALS=$SPARK_HOME/secrets/gcp-credentials.json

RUN pip install delta-spark
```
### Create a new GCP project

1. Create a new GCP project and name it something like `spark-gcs-bq`
2. Authorize the APIs for Google Cloud Storage and BigQuery 
3. Create a new bucket in the Google Cloud Storage named `[project-name]-censo`. Bucket names must be globally unique, so adding the project name as a prefix helps.
4. Switch over to BigQuery and create a new dataset named `censo_ensino_superior`. Make sure it has the same location type as the GCS bucket from the previous step.
5. Create a service account under `IAM & Admin` > `Service Accounts`. Give the service account the following roles (Storage Admin + BigQuery Admin).
6. Create a key by clicking on the menu (three dots) and selecting Add Key > Create New Key > Create. This will download a .json to your computer.

Back to the working directory, create a new file `prepare_env.sh` and add the following which (1) will create directories that Spark will be authorized to access and (2) download the GCS connector for Spark:
```bash
mkdir -p ./data/
mkdir -p ./src/credentials
chmod -R 777 ./src
chmod -R 777 ./data

wget https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-latest-hadoop2.jar
```
Execute `prepare_env.sh`
```bash
source prepare_env.sh
```
Rename the downloaded credentials .json to `gcp-credentials.json` and move it to `./src/credentials`.

Run the container
```bash
docker compose up --build
```

## 1. Downloading data
Let's write a script for downloading data from the [Microdados do Censo da Educação Superior](https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/censo-da-educacao-superior) (Brazilian Higher Education Census).

The Education Census data webpage contains links for data from 1995 to 2022. All download links follow the format `https://download.inep.gov.br/microdados/microdados_censo_da_educacao_superior_{year}.zip`. Let's set this as the base URL and create a list of years from 1995 to 2022 inclusive. We'll also import the os and requests modules
```python
BASE_URL = "https://download.inep.gov.br/microdados/microdados_censo_da_educacao_superior_{}.zip"
YEARS = range(1995, 2022+1)
```
Then let's set the directory where we want to download the zip files. We'll also set a boolean so we can choose whether to unzip the files our not.
```python
SAVE_PATH = os.path.join(os.path.dirname(__file__), "data")
UNZIP = True
```
Now let's write the main download script which uses `os` and the `requests.get()` function to download the zip file for every year, unzip each file, move the csv file to the data directory, and delete the unzipped folder and zip file afterwards.
```python
for year in YEARS:
  print("Downloading data for year {}".format(year))
  url = BASE_URL.format(year)
  r = requests.get(url, allow_redirects=True, verify=False)
  open(os.path.join(SAVE_PATH, "{}.zip".format(year)), "wb").write(r.content)

  # Unzip the file
  if UNZIP:
    print("Unzipping file")
    os.system(f"unzip {SAVE_PATH}/{year}.zip -d {SAVE_PATH}/{year}")

    csvs_path = f"{SAVE_PATH}/{year}/*/dados/*.CSV"
    os.system(f"mv {csvs_path} {SAVE_PATH}")

    # Delete the zip file
    os.system(f"rm {SAVE_PATH}/{year}.zip")
    # Delete the unzipped folder
    os.system(f"rm -rf {SAVE_PATH}/{year}")
```
Bring this all together in the `download_files.py` file, which we'll run to download the data.
```bash
python download_files.py
```
Check the `./data` folder where we can find 
## 2. 