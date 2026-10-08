"""Envoi et copie multipart via s3a:// sur MinIO.

hadoop-aws 3.4.2 demande le bundle complet du SDK AWS (2.29.52) ; on l'exclut au
profit du SDK 2.44.4 d'iceberg-aws-bundle, complété par s3-transfer-manager. Les
petits fichiers d'un checkpoint n'exercent pas le multipart : ce test le force.
"""

import hashlib
import uuid

import boto3
import pytest
from botocore.config import Config
from pyspark.sql import functions as F

pytestmark = [pytest.mark.spark, pytest.mark.integration]

PART_SIZE = 5 * 1024 * 1024  # minimum imposé par S3 et par s3a
CHUNK = bytes(range(256)) * 4096  # 1 Mio, contenu technique sans valeur métier
CHUNKS = 12  # 12 Mio : 3 parts de 5, 5 et 2 Mio
EXPECTED_MD5 = hashlib.md5(CHUNK * CHUNKS).hexdigest()


@pytest.fixture
def s3a(spark, infra):
    jvm = spark._jvm
    conf = jvm.org.apache.hadoop.conf.Configuration(spark._jsc.hadoopConfiguration())
    conf.set("fs.s3a.multipart.size", str(PART_SIZE))
    conf.set("fs.s3a.multipart.threshold", str(PART_SIZE))
    prefix = f"checkpoints/test_tmp/multipart_{uuid.uuid4().hex[:8]}"
    root = jvm.org.apache.hadoop.fs.Path(f"s3a://{infra.bucket}/{prefix}")
    # newInstance : instance à part, pour ne pas hériter du cache ni le polluer.
    fs = jvm.org.apache.hadoop.fs.FileSystem.newInstance(root.toUri(), conf)
    client = boto3.client(
        "s3",
        endpoint_url=infra.s3_endpoint,
        aws_access_key_id=infra.s3_access_key,
        aws_secret_access_key=infra.s3_secret_key,
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )

    def head(name):
        return client.head_object(Bucket=infra.bucket, Key=f"{prefix}/{name}")

    def path(name):
        return jvm.org.apache.hadoop.fs.Path(root, name)

    yield fs, path, head
    fs.delete(root, True)
    fs.close()


def parts(head):
    """Nombre de parts d'un objet : l'ETag d'un envoi multipart se termine par -N."""
    etag = head["ETag"].strip('"')
    return int(etag.rsplit("-", 1)[1]) if "-" in etag else 0


def md5_through_spark(spark, path):
    row = spark.read.format("binaryFile").load(path.toString()).select(F.md5("content")).first()
    return row[0]


def providers(spark, class_file):
    """Jars du classpath Spark qui fournissent une classe donnée."""
    loader = spark._jvm.org.apache.spark.util.Utils.getContextOrSparkClassLoader()
    urls = loader.getResources(class_file)
    found = []
    while urls.hasMoreElements():
        found.append(urls.nextElement().toString().split("!")[0].rsplit("/", 1)[-1])
    return found


def test_classpath_has_one_aws_sdk(spark):
    # Une seule copie de chaque classe du SDK : pas de mélange de versions.
    s3_client = providers(spark, "software/amazon/awssdk/services/s3/S3Client.class")
    transfer = providers(spark, "software/amazon/awssdk/transfer/s3/S3TransferManager.class")
    assert len(s3_client) == 1 and "iceberg-aws-bundle-1.11.0" in s3_client[0], s3_client
    assert transfer == ["s3-transfer-manager-2.44.4.jar"], transfer


def test_multipart_upload_through_output_stream(spark, s3a):
    fs, path, head = s3a
    out = fs.create(path("stream.bin"), True)
    for _ in range(CHUNKS):
        out.write(bytearray(CHUNK))
    out.close()

    uploaded = head("stream.bin")
    assert uploaded["ContentLength"] == CHUNKS * len(CHUNK)
    assert parts(uploaded) == 3
    assert md5_through_spark(spark, path("stream.bin")) == EXPECTED_MD5


def test_multipart_copy_on_rename(spark, s3a):
    # Le renommage d'un checkpoint est une copie côté S3, faite par le
    # S3TransferManager : c'est le module ajouté à la main.
    fs, path, head = s3a
    out = fs.create(path("before.bin"), True)
    for _ in range(CHUNKS):
        out.write(bytearray(CHUNK))
    out.close()

    assert fs.rename(path("before.bin"), path("after.bin"))

    assert not fs.exists(path("before.bin"))
    assert head("after.bin")["ContentLength"] == CHUNKS * len(CHUNK)
    assert md5_through_spark(spark, path("after.bin")) == EXPECTED_MD5


def test_multipart_upload_of_a_local_file(spark, s3a, tmp_path):
    fs, path, head = s3a
    local = tmp_path / "local.bin"
    local.write_bytes(CHUNK * CHUNKS)

    fs.copyFromLocalFile(False, True, spark._jvm.org.apache.hadoop.fs.Path(str(local)), path("local.bin"))

    uploaded = head("local.bin")
    assert uploaded["ContentLength"] == CHUNKS * len(CHUNK)
    assert parts(uploaded) >= 2
    assert md5_through_spark(spark, path("local.bin")) == EXPECTED_MD5
