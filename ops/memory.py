"""Pic de mémoire du processus Python et de la JVM du driver Spark.

La JVM est le gros consommateur en mode local. Trois mesures, à ne pas confondre :
- `jvm_rss_peak_mb` : pic de mémoire résidente réelle du processus JVM (VmHWM de /proc), ce que
  WSL doit pouvoir fournir ; c'est le chiffre à utiliser pour dimensionner la machine ;
- `jvm_heap_peak_mb` : somme des pics de chaque zone du tas Java. Les pics de zones différentes
  n'ont pas lieu au même instant : borne haute de l'occupation du tas, pas une mesure exacte ;
- `python_rss_peak_mb` : pic du processus Python.
"""

import resource


def _vm_hwm_mb(pid: int) -> float | None:
    try:
        with open(f"/proc/{pid}/status", encoding="ascii") as status:
            for line in status:
                if line.startswith("VmHWM:"):
                    return int(line.split()[1]) / 1024  # Ko -> Mo
    except OSError:
        pass
    return None


def peak_memory(spark) -> dict[str, float | None]:
    jvm = spark._jvm
    heap = sum(
        pool.getPeakUsage().getUsed()
        for pool in jvm.java.lang.management.ManagementFactory.getMemoryPoolMXBeans()
        if pool.getType().name() == "HEAP"
    )
    return {
        "jvm_rss_peak_mb": _vm_hwm_mb(int(jvm.java.lang.ProcessHandle.current().pid())),
        "jvm_heap_peak_mb": heap / 1024 / 1024,
        "jvm_heap_max_mb": jvm.java.lang.Runtime.getRuntime().maxMemory() / 1024 / 1024,
        "python_rss_peak_mb": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024,
    }
