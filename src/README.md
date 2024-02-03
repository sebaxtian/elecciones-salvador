# Web Scraping - Elecciones Salvador

Scraping process to download each summary vote totals (JPG or PDF files) published by the Salvador government. There are 16 million votes approximately and the summary vote totals will be used by the independent project of the Guatemalan non-profit organization [FundacionHCG.org](https://fundacionhcg.org/) and [FiscalDigital.net](https://fiscaldigital.net/)

## Environment Variables

Please setup each environment variable value before execute any script or python file:

Create a new **.env** file inside the **src** directory and use the environment variables as you can see in the **example.env** file just change the values. This file will be ignored and never will be committed to the repository.

The environement variables below are required:

* BUCKET_NAME=example-bucket-name
* AWS_ACCESS_KEY_ID=EXAMPLE1234DEMO
* AWS_SECRET_ACCESS_KEY=ExAmPle1d2Em3o4Acce56ss7Key
* AWS_DEFAULT_REGION=us-west-1
* BROWSER_PATH=src/scraping/browser/app/chrome-linux64/chrome
* BROWSER_DRIVER_PATH=src/scraping/browser/driver/chromedriver-linux64/chromedriver

## Data
Read more [README.md](data/README.md)

## Scripts

Execute each script from the elecciones-salvador directory root.

### AWS S3 CLI

Provides methods for listing, uploading, downloading, and deleting files in an S3 bucket.

```bash
# from elecciones-salvador root directory
$promt> python src/aws/awss3.py --help
```

#### AWS S3 CLI: List

List all objects in the bucket.

```bash
# from elecciones-salvador root directory
$promt> python src/aws/awss3.py --list
```

#### AWS S3 CLI: Upload

Upload a file to a bucket.

```bash
# from elecciones-salvador root directory
$promt> python src/aws/awss3.py --upload src/data/0_raw/bike.jpg
```

#### AWS S3 CLI: Download

Download a file from a bucket.

```bash
# from elecciones-salvador root directory
$promt> python src/aws/awss3.py --download bike.jpg
```

#### AWS S3 CLI: Delete

Delete a file from a bucket.

```bash
# from elecciones-salvador root directory
$promt> python src/aws/awss3.py --delete bike.jpg
```
