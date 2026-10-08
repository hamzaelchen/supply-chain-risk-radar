"""Résolution des jars Spark dans un dossier local : `make jars`.

Pourquoi ne pas laisser faire `spark.jars.packages` : PySpark recopie alors tous
les jars résolus dans un dossier temporaire à chaque session. Avec les 709 Mo du
SDK AWS, cela coûtait environ 20 s par lancement. Ici, on résout une fois avec le
même outil que Spark (Ivy, livré avec pyspark), puis la session met directement
le dossier sur le classpath du driver.

Le dossier porte l'empreinte des versions demandées : changer une version change
de dossier, et la session refuse de démarrer tant que `make jars` n'a pas été
relancé.
"""

import hashlib
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from xml.sax.saxutils import quoteattr

from lakehouse.spark_session import SPARK_PACKAGES

ROOT = Path(__file__).resolve().parent.parent
JARS_ROOT = ROOT / "jars"
# Même cache que celui de Spark : ce qui a déjà été téléchargé ne l'est pas deux fois.
IVY_CACHE = Path.home() / ".ivy2.5.2" / "cache"
COMPLETE_MARKER = ".complete"

# Dépendances transitives que pyspark livre déjà, à la même version. On ne les
# reprend pas : le classpath du driver passe avant les jars de Spark, et on ne
# veut y mettre que ce qui manque à Spark.
PROVIDED_BY_SPARK = [
    "org.apache.spark:spark-tags_2.13",
    "org.apache.hadoop:hadoop-client-api",
    "org.apache.hadoop:hadoop-client-runtime",
    "org.slf4j:slf4j-api",
    "org.xerial.snappy:snappy-java",
    "org.lz4:lz4-java",
    "com.google.code.findbugs:jsr305",
    "org.scala-lang.modules:scala-parallel-collections_2.13",
]


class LocalJarsMissing(RuntimeError):
    pass


def jars_hash(packages: list[str] = SPARK_PACKAGES, excluded: list[str] = PROVIDED_BY_SPARK) -> str:
    """Empreinte de ce qui est demandé à Ivy : versions et exclusions."""
    spec = "\n".join(["packages", *sorted(packages), "excluded", *sorted(excluded)])
    return hashlib.sha256(spec.encode()).hexdigest()[:12]


def jars_dir(root: Path = JARS_ROOT) -> Path:
    return root / jars_hash()


def require_local_jars(root: Path = JARS_ROOT) -> Path:
    """Dossier des jars pour les versions courantes, ou une erreur qui dit quoi faire."""
    directory = root / jars_hash()
    if not (directory / COMPLETE_MARKER).is_file():
        raise LocalJarsMissing(
            f"Jars Spark introuvables dans {directory}.\n"
            "Lancer `make jars` (à refaire après chaque changement de version de "
            "Spark, Iceberg, Hadoop ou du SDK AWS)."
        )
    return directory


def ivy_xml(packages: list[str], excluded: list[str]) -> str:
    dependencies = []
    for package in packages:
        org, name, rev = package.split(":")
        dependencies.append(
            f"    <dependency org={quoteattr(org)} name={quoteattr(name)} rev={quoteattr(rev)}"
            # Même correspondance de configurations que spark.jars.packages.
            ' conf="default->default(runtime)"/>'
        )
    excludes = []
    for item in excluded:
        org, module = item.split(":")
        excludes.append(f"    <exclude org={quoteattr(org)} module={quoteattr(module)}/>")
    return "\n".join(
        [
            '<ivy-module version="2.0">',
            '  <info organisation="supply-chain-risk-radar" module="spark-jars"/>',
            "  <dependencies>",
            *dependencies,
            *excludes,
            "  </dependencies>",
            "</ivy-module>",
            "",
        ]
    )


# Un seul dépôt, Maven Central, nommé comme dans le cache de Spark : résolution
# identique d'une machine à l'autre, et les métadonnées déjà en cache sont reconnues.
IVY_SETTINGS = """<ivysettings>
  <settings defaultResolver="central"/>
  <resolvers>
    <ibiblio name="central" m2compatible="true"/>
  </resolvers>
</ivysettings>
"""


def _ivy_jar() -> Path:
    import pyspark

    candidates = sorted((Path(pyspark.__file__).parent / "jars").glob("ivy-*.jar"))
    if not candidates:
        raise RuntimeError("ivy-*.jar introuvable dans l'installation de pyspark")
    return candidates[-1]


def resolve(root: Path = JARS_ROOT) -> Path:
    target = root / jars_hash()
    if (target / COMPLETE_MARKER).is_file():
        print(f"jars déjà résolus : {target}")
        return target

    root.mkdir(exist_ok=True)
    # On remplit un dossier provisoire et on ne le renomme qu'une fois complet :
    # un téléchargement interrompu ne laisse jamais un dossier à moitié rempli
    # qui passerait pour valide.
    staging = Path(tempfile.mkdtemp(prefix=f".{target.name}-", dir=root))
    try:
        descriptor = staging / "ivy.xml"
        descriptor.write_text(ivy_xml(SPARK_PACKAGES, PROVIDED_BY_SPARK))
        settings = staging / "ivysettings.xml"
        settings.write_text(IVY_SETTINGS)
        subprocess.run(
            [
                "java", "-jar", str(_ivy_jar()),
                "-settings", str(settings),
                "-ivy", str(descriptor),
                "-cache", str(IVY_CACHE),
                "-confs", "default",
                "-types", "jar", "bundle",
                "-retrieve", str(staging / "[artifact]-[revision](-[classifier]).[ext]"),
            ],
            check=True,
        )  # fmt: skip
        jars = sorted(p.name for p in staging.glob("*.jar"))
        if not jars:
            raise RuntimeError("Ivy n'a récupéré aucun jar")
        descriptor.unlink()
        settings.unlink()
        (staging / COMPLETE_MARKER).write_text("\n".join(jars) + "\n")
        if target.exists():
            shutil.rmtree(target)
        staging.rename(target)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise

    size_mb = sum(p.stat().st_size for p in target.glob("*.jar")) // 1_000_000
    print(f"{len(jars)} jars ({size_mb} Mo) dans {target}")
    for name in jars:
        print(f"  {name}")
    return target


if __name__ == "__main__":
    resolve()
    sys.exit(0)
