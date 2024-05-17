import hashlib
import os
import sys
import tempfile
import threading
import time
from datetime import datetime, timezone
from enum import Enum
from multiprocessing import Pool
from pathlib import Path

import boto3
import numpy as np
import pandas as pd
import pendulum
import pygame
import requests
import sounddevice as sd
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from gtts import gTTS
from loguru import logger
from tqdm import tqdm

# Load .env variables
_ = load_dotenv(dotenv_path=f"{Path().resolve()}/src/.env")


logger.add(
    "src/logs/elecciones_{time:!UTC}.log",
    rotation="20 MB",
    level="DEBUG",
    format="{time:!UTC} - {level} - {name} - {message}",
)


# Bucket Name
BUCKET_NAME = os.getenv("BUCKET_NAME", None)

# AWS Session
AWS_SESSION = boto3.Session(
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", None),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", None),
    region_name=os.getenv("AWS_DEFAULT_REGION", None),
)

# Client S3
S3_CLIENT = AWS_SESSION.client("s3")


class ActaStatus(Enum):
    PENDING = "pending"
    DOWNLOADED = "downloaded"
    NOT_FOUND = "not_found"
    FORBIDDEN = "forbidden"
    ERROR = "error"

    def get_status(self, value):
        for status in ActaStatus:
            if status.value == value:
                return status


class ActaURL(Enum):
    ALCALDE = "https://divulgacion.tse.gob.sv/actas/ALCALDE"
    DIP_PARLACEN = "https://divulgacion.tse.gob.sv/actas/DIP_PARLACEN"


class Acta:

    def __init__(
        self,
        url,
        status=ActaStatus.PENDING,
        uploaded=False,
        datetime=None,
        file_names=[],
        hashes=[],
    ):
        self.__url = url
        self.__status = status
        self.__uploaded = uploaded
        self.__datetime = datetime
        self.__file_names = file_names
        self.__hashes = hashes

    # Getters and Setters
    @property
    def url(self):
        return self.__url

    @url.setter
    def url(self, url):
        self.__url = url

    @property
    def status(self):
        return self.__status

    @status.setter
    def status(self, status):
        self.__status = status

    @property
    def uploaded(self):
        return self.__uploaded

    @uploaded.setter
    def uploaded(self, uploaded):
        self.__uploaded = uploaded

    @property
    def datetime(self):
        return self.__datetime

    @datetime.setter
    def datetime(self, datetime):
        self.__datetime = datetime

    @property
    def file_names(self):
        return self.__file_names

    @file_names.setter
    def file_names(self, file_names):
        self.__file_names = file_names

    @property
    def hashes(self):
        return self.__hashes

    @hashes.setter
    def hashes(self, hashes):
        self.__hashes = hashes

    def __str__(self):
        return f"URL: {self.url}, STATUS: {self.status}, UPLOADED: {self.uploaded}, DATETIME: {self.datetime}, FILE_NAMES: {self.file_names}, HASHES: {self.hashes}"  # noqa: E501

    def __repr__(self):
        return f"{self.url}, {self.status}, {self.uploaded}, {self.datetime}, {self.file_names}, {self.hashes}"  # noqa: E501


class DataSources:

    columns = ["URL", "STATUS", "UPLOADED", "DATETIME", "FILE_NAMES", "HASHES"]

    def __init__(self):
        self.actas = []

    def add_acta(self, acta):
        self.actas.append(acta)

    @logger.catch
    def to_df(self):
        return pd.DataFrame(
            [
                [
                    acta.url,
                    acta.status.value,
                    acta.uploaded,
                    acta.datetime,
                    " ".join(acta.file_names),
                    " ".join(acta.hashes),
                ]
                for acta in self.actas
            ],
            columns=DataSources.columns,
        )

    @logger.catch
    def save(self, file_name):
        logger.info(f"Saving {file_name} ...")
        self.to_df().to_csv(file_name, index=False)
        logger.info(f"{file_name} saved, OK")

    def load(self, file_name):
        logger.info(f"Loading {file_name} ...")
        df = pd.read_csv(file_name, usecols=DataSources.columns)
        self.actas = [
            Acta(
                url=row["URL"],
                status=next(
                    (status for status in ActaStatus if status.value == row["STATUS"]),
                    ActaStatus.ERROR,
                ),
                uploaded=row["UPLOADED"],
                datetime=row["DATETIME"],
                file_names=(
                    row["FILE_NAMES"].split(" ")
                    if isinstance(row["FILE_NAMES"], str)
                    else []
                ),
                hashes=(
                    row["HASHES"].split(" ")
                    if isinstance(row["FILE_NAMES"], str)
                    else []
                ),
            )
            for _, row in df.iterrows()
        ]
        logger.info(f"{file_name} loaded, OK")

    def __str__(self):
        return f"Total Actas: {len(self.actas)}"

    def __repr__(self):
        return f"Total Actas: {len(self.actas)}"


# Progress percentage upload to S3
class ProgressPercentageUploadToS3(object):
    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)"
                % (self._filename, self._seen_so_far, self._size, percentage)
            )
            sys.stdout.flush()


# Play sound when the script finishes
def play_beep(frequency=1000, duration=3, sampling_rate=44100):
    # Generate audio data for the beep sound
    tempo = np.arange(0, duration, 1 / sampling_rate)
    sound = 0.5 * np.sin(2 * np.pi * frequency * tempo)

    # Play the beep sound
    sd.play(sound, sampling_rate)
    sd.wait()


# Text to speech
def text_to_speech(message, language="es"):
    # Crea un objeto gTTS con el texto y el idioma especificados
    tts = gTTS(text=message, lang=language, slow=False)

    # Guarda el archivo de audio temporalmente
    archivo_temporal = tempfile.NamedTemporaryFile(delete=False)
    tts.save(archivo_temporal.name)

    # Inicializa pygame y reproduce el archivo de audio
    pygame.init()
    pygame.mixer.init()
    pygame.mixer.music.load(archivo_temporal.name)
    pygame.mixer.music.play()

    # Espera hasta que la reproducción termine
    while pygame.mixer.music.get_busy():
        pygame.time.Clock().tick(10)

    # Cierra pygame y elimina el archivo temporal
    pygame.mixer.quit()
    os.remove(archivo_temporal.name)


# Create function to download the acta
@logger.catch
def download_acta(acta):
    # Get the isoformat datetime
    acta.datetime = datetime.now(timezone.utc).isoformat(
        sep="T", timespec="milliseconds"
    )
    try:
        # URL Dashboard Request
        url_dashboard = acta.url
        # Dashboard Type
        dashboard_type = None
        # Check the URL Dashboard Type
        if url_dashboard.find("-4.html") != -1:
            # ALCALDE
            dashboard_type = ActaURL.ALCALDE.value
        elif url_dashboard.find("-2.html") != -1:
            # DIP PARLACEN
            dashboard_type = ActaURL.DIP_PARLACEN.value
        else:
            # URL Type not found
            logger.error(f"URL Dashboard Type not found: {url_dashboard}")
            acta.status = ActaStatus.ERROR
            return acta

        # Get the files names from the dashboard
        dashboard_file_names = get_file_names_from_dashboard(url_dashboard)

        # If the dashboard file names is empty
        if not dashboard_file_names:
            acta.status = ActaStatus.NOT_FOUND
            return acta

        # List of hashes and file names of the acta
        hashes = []
        file_names = []

        # For each file name
        for dashboard_file_name in dashboard_file_names:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.3"  # noqa: E501
            }
            response = requests.get(
                f"{dashboard_type}/{dashboard_file_name}", headers=headers
            )
            # If the status code is 200
            if response.status_code == 200:
                # If the response content is not empty
                if response.content:
                    file_hash = hashlib.sha256(response.content).hexdigest()
                    hashes.append(file_hash)
                    file_name = f"{file_hash}.jpeg"
                    # If the file does exist
                    if os.path.exists(f"src/data/0_raw/{file_name}"):
                        duplicated_file_name = f"{file_hash}_{acta.datetime}.jpeg"
                        # Save the acta file in the raw folder
                        with open(
                            f"src/data/0_duplicates/{duplicated_file_name}", "wb"
                        ) as f:
                            f.write(response.content)
                    else:
                        # Save the acta file in the raw folder
                        with open(f"src/data/0_raw/{file_name}", "wb") as f:
                            f.write(response.content)
                    # Append the file name to the list
                    file_names.append(file_name)
                else:
                    acta.status = ActaStatus.NOT_FOUND
            # If the status code is 404
            elif response.status_code == 404:
                acta.status = ActaStatus.NOT_FOUND
            # If the status code is 403
            elif response.status_code == 403:
                acta.status = ActaStatus.FORBIDDEN
            # If the status code is different to 200, 404 or 403
            else:
                acta.status = ActaStatus.ERROR
        # Acta Downloaded
        acta.hashes = hashes
        acta.file_names = file_names
        acta.status = ActaStatus.DOWNLOADED
        # Upload the acta file to the S3 bucket
        acta = upload_acta_to_s3(acta)
    except Exception:
        acta.status = ActaStatus.ERROR
    # logger.info(f"Acta: {acta}")
    # Return the acta
    return acta


# Upload the acta file to the S3 bucket
@logger.catch
def upload_acta_to_s3(acta):
    try:
        acta.uploaded = False
        # For each file name
        for file_name in acta.file_names:
            # logger.info(f"Uploading {file_name} to S3 ...")
            # Upload the acta file to the S3 bucket
            S3_CLIENT.upload_file(
                f"src/data/0_raw/{file_name}",
                BUCKET_NAME,
                file_name,
                Callback=ProgressPercentageUploadToS3(f"src/data/0_raw/{file_name}"),
            )
            # logger.info(f"{file_name} uploaded to S3, OK")
        acta.uploaded = True
    except NoCredentialsError as e:
        logger.error(f"NoCredentialsError: No AWS credentials found. {e}")
    except PartialCredentialsError as e:
        logger.error(
            f"PartialCredentialsError: Partial AWS credentials found. {e}"  # noqa: E501
        )
    except ClientError as e:
        logger.error(f"ClientError: Error uploading file to S3. {e}")
    except Exception as e:
        logger.error(f"Exception: Error uploading file to S3. {e}")
    # Return the acta
    return acta


# Create function to process the actas
def process_acta(acta, callback=None):
    # If the acta is not downloaded
    if acta.status != ActaStatus.DOWNLOADED:
        # Pedding acta
        acta.status = ActaStatus.PENDING
        # Download the acta
        acta = download_acta(acta)
    elif acta.status == ActaStatus.DOWNLOADED and not acta.uploaded:
        # Upload the acta file to the S3 bucket
        acta = upload_acta_to_s3(acta)
    # Callback progress
    callback()
    # Return the acta
    return acta


# Create function to process the chunk
def process_chunk(args):  # -> list:
    # Get the index and chunk
    index, chunk = args
    # Progress bar
    pbar = tqdm(total=len(chunk), desc=f"Processing Chunk {index}", ascii="░▒█")

    # Callback function
    def callback(*_):
        pbar.update()

    actas = [process_acta(acta, callback=callback) for acta in chunk]

    # Save each acta in a dataframe and a file
    # Close the progress bar
    pbar.close()

    # Save each acta in a dataframe and a file
    pd.DataFrame(
        [
            [
                acta.url,
                acta.status.value,
                acta.uploaded,
                acta.datetime,
                " ".join(acta.file_names),
                " ".join(acta.hashes),
            ]
            for acta in actas
        ],
        columns=DataSources.columns,
    ).to_csv(f"src/data/chunk_{index}.csv", index=False)

    # Return the actas
    return actas


# Create function to process the data sources
@logger.catch
def process_data_sources(data_sources, chunk_size=100):
    logger.info(f"Chunk Size: {chunk_size}")

    # Chunks of 100 actas
    chunks = [
        data_sources.actas[i : i + chunk_size]
        for i in range(0, len(data_sources.actas), chunk_size)
    ]

    logger.info(f"Total Chunks: {len(chunks)}")

    # Number of processes
    num_processes = 12

    logger.info(f"Number of Processes: {num_processes}")

    # Process each chunk
    with Pool(num_processes) as pool:
        # Process each chunk
        results = pool.map(process_chunk, enumerate(chunks))

    # Update the data sources
    data_sources.actas = [acta for chunk in results for acta in chunk]

    # Return the data sources
    return data_sources


# Get file names from dashboard
def get_file_names_from_dashboard(url_dashboard):
    try:
        # Realizar la solicitud HTTP GET para obtener el contenido HTML de la página
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.3"  # noqa: E501
        }
        respuesta = requests.get(url_dashboard, headers=headers)
        respuesta.raise_for_status()  # Verificar si la solicitud fue exitosa

        # Analizar el contenido HTML con BeautifulSoup
        soup = BeautifulSoup(respuesta.text, "html.parser")

        # Encontrar todos los enlaces (etiqueta 'a') que contienen la extensión deseada
        enlaces_imagenes = soup.select("#images img")
        # print("enlaces_imagenes:", enlaces_imagenes)

        # Descargar cada imagen encontrada
        file_names = [enlace["src"].split("/")[-1] for enlace in enlaces_imagenes]
        # logger.info(f"url_dashboard: {url_dashboard}, file_names: {file_names}")

        # return file_names
        return file_names

    except requests.exceptions.HTTPError as errh:
        logger.error(f"Error HTTP: {errh}")
        raise errh
    except requests.exceptions.ConnectionError as errc:
        logger.error(f"Error de conexión: {errc}")
        raise errc
    except requests.exceptions.Timeout as errt:
        logger.error(f"Error de tiempo de espera: {errt}")
        raise errt
    except requests.exceptions.RequestException as err:
        logger.error(f"Error en la solicitud: {err}")
        raise err


def init_data_sources_alcalde(data_sources, TOTAL=8562, START=1):
    logger.info("Initializing Alcalde data_sources ...")
    for number in range(START, TOTAL + 1):
        URL_DASHBOARD = f"https://divulgacion.tse.gob.sv/dashboard-jrv-{number}-4.html"
        data_sources.add_acta(Acta(URL_DASHBOARD))
    logger.info("data_sources Alcalde initialized, OK")
    return data_sources


def init_data_sources_dip_parlacen(data_sources, TOTAL=8562, START=1):
    logger.info("Initializing DIP PARLACEN data_sources ...")
    for number in range(START, TOTAL + 1):
        URL_DASHBOARD = f"https://divulgacion.tse.gob.sv/dashboard-jrv-{number}-2.html"
        data_sources.add_acta(Acta(URL_DASHBOARD))
    logger.info("data_sources DIP PARLACEN initialized, OK")
    return data_sources


def marzo(total=8562, start=1, chunk_size=1000, datasources_file=None, speech=True):
    """
    Elecciones de Diputaciones al Parlamento Centroamericano e integrantes de los Consejos Municipales
    """  # noqa: E501

    start_datetime = datetime.now(timezone.utc).isoformat(
        sep="T", timespec="milliseconds"
    )
    logger.info(f"Start: {start_datetime}")
    logger.info("Running marzo ...")

    # Create a marzo DataSources
    data_sources = DataSources()

    if datasources_file:
        data_sources.load(datasources_file)
        logger.info(f"DataSources loaded from {datasources_file}, OK")
    else:
        logger.info("DataSources not loaded from file, OK")
        # Initialize the dataframe with the URLs of the actas from 1 to 8562
        init_data_sources_alcalde(data_sources, total, start)
        init_data_sources_dip_parlacen(data_sources, total, start)

    # Process the data sources
    data_sources = process_data_sources(data_sources, chunk_size=chunk_size)

    # Get the total actas
    total_actas = len(data_sources.actas)
    logger.info(f"Total Actas: {total_actas}")

    # Count the actas with status downloaded
    total_actas_downloaded = len(
        [acta for acta in data_sources.actas if acta.status == ActaStatus.DOWNLOADED]
    )
    logger.info(f"Total Actas Downloaded: {total_actas_downloaded}")

    # Count the actas with status uploaded
    total_actas_uploaded = len(
        [acta for acta in data_sources.actas if acta.uploaded is True]
    )
    logger.info(f"Total Actas Uploaded: {total_actas_uploaded}")

    # Count the actas with status not found
    total_actas_not_found = len(
        [acta for acta in data_sources.actas if acta.status == ActaStatus.NOT_FOUND]
    )
    logger.info(f"Total Actas Not Found: {total_actas_not_found}")

    # Count the actas with status forbidden
    total_actas_forbidden = len(
        [acta for acta in data_sources.actas if acta.status == ActaStatus.FORBIDDEN]
    )
    logger.info(f"Total Actas Forbidden: {total_actas_forbidden}")

    # Count the actas with status error
    total_actas_error = len(
        [acta for acta in data_sources.actas if acta.status == ActaStatus.ERROR]
    )
    logger.info(f"Total Actas Error: {total_actas_error}")

    # From data_sources.to_df() count the total actas with duplicates and status downloaded
    total_actas_duplicated = (
        data_sources.to_df()[data_sources.to_df()["STATUS"] == "downloaded"]
        .duplicated(subset="URL")
        .sum()
    )
    logger.info(
        f"Total Actas with Duplicates and Downloaded from data_sources.to_df(): {total_actas_duplicated}"  # noqa: E501
    )

    logger.info("marzo finished, OK")
    end_datetime = datetime.now(timezone.utc).isoformat(
        sep="T", timespec="milliseconds"
    )
    logger.info(f"End: {end_datetime}")

    # Save the datasources
    ds_file_name = f"marzo_{end_datetime}.csv"
    data_sources.save(f"src/data/{ds_file_name}")

    # Get dataframe from data_sources.to_df() with duplicates
    # data_sources.to_df()[data_sources.to_df()["STATUS"] == "downloaded"].duplicated(
    #     subset="URL"
    # ).to_csv(f"src/data/duplicated_{ds_file_name}", index=False)

    # Total files downloaded
    total_files = 0
    total_files_uploaded = 0
    for acta in data_sources.actas:
        if acta.status == ActaStatus.DOWNLOADED:
            total_files += len(acta.file_names)
        if acta.status == ActaStatus.DOWNLOADED and acta.uploaded:
            total_files_uploaded += len(acta.file_names)
    logger.info(f"Total Files Downloaded: {total_files}")
    logger.info(f"Total Files Uploaded: {total_files_uploaded}")

    pendulum.set_locale("es")
    dt_diff = pendulum.parse(end_datetime).diff_for_humans(
        pendulum.parse(start_datetime)
    )
    pendulum.set_locale("en")
    logger.info(f"Proceso Terminado {dt_diff}")

    # Play sound and speech when the script finishes
    if speech:
        # Play sound when the script finishes
        # play_beep()

        # Speech the message
        # text_to_speech(f"Proceso Terminado {dt_diff}")
        text_to_speech(f"Total Actas: {total_actas}")
        # text_to_speech(f"Total Actas Descargadas: {total_actas_downloaded}")
        # text_to_speech(f"Total Actas Cargadas: {total_actas_uploaded}")
        # text_to_speech(f"Total Actas No Encontradas: {total_actas_not_found}")
        # text_to_speech(f"Total Actas Prohibidas: {total_actas_forbidden}")
        # text_to_speech(f"Total Actas con Error: {total_actas_error}")
        # text_to_speech(f"Total Actas Duplicadas: {total_actas_duplicated}")
        text_to_speech(f"Total Archivos Descargados: {total_files}")
        text_to_speech(f"Total Archivos Cargados: {total_files_uploaded}")
        text_to_speech("Proceso Terminado")

        # Play sound when the script finishes
        # play_beep()

    # Return the ds_file_name
    return ds_file_name


if __name__ == "__main__":
    # Delete jpeg's
    # for jpeg in src/data/0_raw/*.jpeg; do rm -v "$jpeg"; done
    # Count jpeg's
    # ls -l src/data/0_raw/ | grep '^-' | wc -l

    # Run the febrero function
    # ds_file_name = febrero(TOTAL_UNO=100, TOTAL_DOS=100, chunk_size=20)
    # ds_file_name = "febrero_2024-03-01T20:56:40.204+00:00.csv"
    # febrero(
    #     datasources_file=f"src/data/{ds_file_name}",
    #     chunk_size=20,
    # )  # noqa: E501

    # Total Actas: 8562 ALCALDE, 1 archivo por acta
    # Total Actas: 8562 DI PARLACEN, 4 archivos por acta
    # Total Archivos Estimados: 42810
    ds_file_name = marzo(total=8562, start=1, chunk_size=500)
    # ds_file_name = "marzo_2024-03-06T16:59:15.865+00:00.csv"

    while True:
        text_to_speech("Iniciando Proceso")
        ds_file_name = marzo(
            total=8562,
            start=1,
            chunk_size=500,
            datasources_file=f"src/data/{ds_file_name}",
            speech=True,
        )
        text_to_speech("El proceso se ejecutará nuevamente en 1 hora")
        time.sleep(3600)
