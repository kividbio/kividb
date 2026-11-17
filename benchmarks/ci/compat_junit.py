"""JUnit XML for a harness that runs its own checks.

Some harnesses in benchmarks/*-compat drive the framework's API themselves instead of
running its test suite, and keep each check as a row: {"check": "Group: what",
"status": "PASS" | "WARN" | "FAIL", "detail": ..., "ms": ...}. write_junit writes those rows
as JUnit XML, which is what compare.py reads in the compat workflow (benchmarks/README.md).
A harness imports it from here only when asked for --junitxml, so it still runs on its own.
"""

import xml.etree.ElementTree as ET


def write_junit(path, suite, results, seconds):
    """Write results (the rows above) to path as JUnit XML, one <testcase> per check. A
    check named "Group: what" is classname "<suite>.Group" and name "what", so the same
    check has the same name against every server. Anything but PASS is a <failure>."""
    failed = sum(1 for row in results if row["status"] != "PASS")
    root = ET.Element("testsuites")
    testsuite = ET.SubElement(root, "testsuite", {
        "name": f"{suite}-compat",
        "tests": str(len(results)),
        "failures": str(failed),
        "errors": "0",
        "skipped": "0",
        "time": f"{seconds:.3f}",
    })
    for row in results:
        group, sep, name = row["check"].partition(": ")
        if not sep:
            group, name = "", group
        case = ET.SubElement(testsuite, "testcase", {
            "classname": f"{suite}.{group}" if group else suite,
            "name": name,
            "time": f"{row['ms'] / 1000:.3f}",
        })
        if row["status"] != "PASS":
            failure = ET.SubElement(case, "failure", {"message": str(row["detail"])[:400]})
            failure.text = str(row["detail"])
    ET.ElementTree(root).write(path, encoding="utf-8", xml_declaration=True)
