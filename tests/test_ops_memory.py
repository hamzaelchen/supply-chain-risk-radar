import os

import pytest

from ops import memory


def test_vm_hwm_of_this_process_is_positive():
    assert memory._vm_hwm_mb(os.getpid()) > 10


def test_vm_hwm_of_a_missing_process_is_none():
    assert memory._vm_hwm_mb(2**22 + 1) is None


@pytest.mark.spark
def test_peak_memory_reports_the_jvm_and_python_sides(spark):
    got = memory.peak_memory(spark)
    assert set(got) == {
        "jvm_rss_peak_mb",
        "jvm_heap_peak_mb",
        "jvm_heap_max_mb",
        "python_rss_peak_mb",
    }
    assert got["jvm_rss_peak_mb"] > got["jvm_heap_peak_mb"] * 0.5 > 0
    assert got["jvm_heap_max_mb"] >= 1000  # le tas maximal demandé (SPARK_DRIVER_MEMORY)
