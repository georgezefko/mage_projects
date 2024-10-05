# mage_projects
# Data Engineering Projects

This repository aims to demonstrate how to build data pipelines and systems, providing a better understanding of concepts such as ETL, data lakes, and their roles in a data system. The core technologies used are Mage and Docker, upon which we will build and integrate other services to enhance our exploration and understanding.

### Repository structure

- Mage: This directory contains all the files and scripts necessary to execute the pipelines. For installation instructions, refer to the official [Mage documentation](https://docs.mage.ai/getting-started/setup) or the first [tutorial](https://medium.com/data-engineer-things/building-a-local-data-lake-from-scratch-with-minio-iceberg-spark-starrocks-mage-and-docker-c12436e6ff9d), which provides a detailed guide on installing Mage.
- Dockerfile: We use this file to basically run Mage -Note that it contains few Spark specific commands that are not necessary for projects without Spark interactions
- Makefile: This is where all the commands that we will use commonly (you can add yours)
- Docker-Compose: This is the file we use to include the services we want to run every time. At the moment it contains all services I use but you can adjust it based on your needs.

To get full understanding of how to build the repository from scratch you can check the turotial [here](https://medium.com/data-engineer-things/building-a-local-data-lake-from-scratch-with-minio-iceberg-spark-starrocks-mage-and-docker-c12436e6ff9d) or you can simply clone the repo and start from there.


## Tutorials - Projects

### 1. Building a Local Data Lake from scratch with MinIO, Iceberg, Spark, StarRocks, Mage, and Docker

In the first tutorial/project, I guide you through building the repository using Mage as the main orchestrator. We will leverage various technologies to create your local data lake with Iceberg and query your data using StarRocks.

You can find the relevant article with a detailed guide here: [Medium blog](https://medium.com/data-engineer-things/building-a-local-data-lake-from-scratch-with-minio-iceberg-spark-starrocks-mage-and-docker-c12436e6ff9d)

The isolated code for that project is here: [SparkDataLake](https://github.com/georgezefko/mage_projects/tree/feat/sparkDataLake)

### 2. Data Pipeline Development with MinIO, Iceberg, Nessie, Polars, StarRocks, Mage, and Docker

In the second tutorial/project, we leverage the structure we implemented in the first tutorial and utilize Nessie catalogue to build an end to end pipeline using the medallion architecture for structuring our data.

You can find the relevant article with a detailed guide here: [Medium blog]()

The isolated code for that project is here: [IcebergNessie](https://github.com/georgezefko/mage_projects/tree/feat/icebergNessie)

A small practicality for this project is that you can either run the pipelines one by one or just trigger the bronze one and if successfull it will trigger silver and gold. Then you can see the results in MinIO, Nessie and query them from any SQL engine you like.