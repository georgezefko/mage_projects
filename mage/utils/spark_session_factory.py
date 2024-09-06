from pyspark.sql import SparkSession
from abc import ABC, abstractmethod
from delta import configure_spark_with_delta_pip


class SparkSessionFactory(ABC):
    @abstractmethod
    def create_spark_session(self):
        pass

    @abstractmethod
    def configure_s3(self):
        pass


class IcebergSparkSession:
    def __init__(self, app_name, warehouse_path, s3_endpoint, s3_access_key, s3_secret_key):
        self.app_name = app_name
        self.warehouse_path = warehouse_path
        self.s3_endpoint = s3_endpoint
        self.s3_access_key = s3_access_key
        self.s3_secret_key = s3_secret_key
        self.spark = self.create_spark_session()
        self.configure_s3()

    def create_spark_session(self):
        packages = [
            "hadoop-aws-3.3.4",
            'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12-1.5.2',
            'aws-java-sdk-bundle-1.12.262'
        ]

        builder = SparkSession.builder.appName(self.app_name) \
            .config("spark.jars.packages", ",".join(packages)) \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.local.type", "hadoop") \
            .config("spark.sql.catalog.local.warehouse", self.warehouse_path) \
            .config("spark.ui.port", "4050")

        return builder.getOrCreate()

    def configure_s3(self):
        sc = self.spark.sparkContext
        sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", self.s3_access_key)
        sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", self.s3_secret_key)
        sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", self.s3_endpoint)
        sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
        sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
        sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

class NessieIcebergSparkSession:
    def __init__(self, app_name, warehouse_path, s3_endpoint, s3_access_key, s3_secret_key, nessie_uri, aws_region):
        self.app_name = app_name
        self.warehouse_path = warehouse_path
        self.s3_endpoint = s3_endpoint
        self.s3_access_key = s3_access_key
        self.s3_secret_key = s3_secret_key
        self.nessie_uri = nessie_uri
        self.aws_region = aws_region
        self.spark = self.create_spark_session()
        self.configure_s3()

    def create_spark_session(self):
        packages = [
            "hadoop-aws-3.3.4",
            'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12-1.5.2',
            'aws-java-sdk-bundle-1.12.262',
            'nessie-spark-extensions-3.5_2.12-0.91.3',
            'url-connection-client-2.26.15'
        ]

        builder = SparkSession.builder.appName(self.app_name) \
            .config("spark.jars.packages", ",".join(packages)) \
            .config("spark.sql.extensions", 
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
                    "org.projectnessie.spark.extensions.NessieSparkSessionExtensions") \
            .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.nessie.uri", self.nessie_uri) \
            .config("spark.sql.catalog.nessie.ref", "main") \
            .config("spark.sql.catalog.nessie.authentication.type", "NONE") \
            .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
            .config("spark.sql.catalog.nessie.s3.endpoint", self.s3_endpoint) \
            .config("spark.sql.catalog.nessie.warehouse", self.warehouse_path) \
            .config("spark.sql.catalog.nessie.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
            .config("spark.ui.port", "4050")\
            .config("spark.hadoop.fs.s3a.region", self.aws_region)


        return builder.getOrCreate()

    def configure_s3(self):
        sc = self.spark.sparkContext
        sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", self.s3_access_key)
        sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", self.s3_secret_key)
        sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", self.s3_endpoint)
        sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
        sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
        sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        sc._jsc.hadoopConfiguration().set("fs.s3a.region", self.aws_region)




class DeltaSparkSession(SparkSessionFactory):
    def __init__(self, app_name, s3_endpoint, s3_access_key, s3_secret_key):
        self.app_name = app_name
        self.s3_endpoint = s3_endpoint
        self.s3_access_key = s3_access_key
        self.s3_secret_key = s3_secret_key
        self.spark = self.create_spark_session()
        self.configure_s3()

    def create_spark_session(self):
        extra_packages = [
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "io.delta:delta-core_2.12:2.4.0",
            "aws-java-sdk-bundle-1.12.262",
            'delta-storage-2.4.0'
        ]
        builder = SparkSession.builder.appName(self.app_name) \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        return configure_spark_with_delta_pip(builder, extra_packages=extra_packages).getOrCreate()

    def configure_s3(self):
        sc = self.spark.sparkContext
        sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", self.s3_access_key)
        sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", self.s3_secret_key)
        sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", self.s3_endpoint)
        sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
        sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
        sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

def get_spark_session(session_type, **kwargs):
    if session_type == "iceberg":
        return IcebergSparkSession(**kwargs)
    elif session_type == "delta":
        return DeltaSparkSession(**kwargs)
    elif session_type == "nessie":
        return NessieIcebergSparkSession(**kwargs)
    else:
        raise ValueError("Invalid session type")