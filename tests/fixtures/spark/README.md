# Fixtures Spark

`spark_57438_stacktrace.txt` : trace JVM réelle, non modifiée, capturée le 2026-10-08
avec pyspark 4.1.1 en rejouant un lot déjà commité dans Iceberg (marque `commits/0`
du checkpoint effacée, puis relance du job). Ticket : SPARK-57438, corrigé en 4.1.3.
