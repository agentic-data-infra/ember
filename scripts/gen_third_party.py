#!/usr/bin/env python3
"""Generates third-party/generated/deps.bzl from a CMake build of Axiom.

Also derives fbthrift's link line by configuring scripts/fbthrift_probe
against the installed fbthrift (the one built with Velox's folly).

Velox and Axiom only build with CMake. Rather than re-describing their
(large, frequently changing) dependency graph in Buck, we read the compile
flags and link lines that CMake computed and hand them to Buck as
prebuilt_cxx_library flags. Re-run after rebuilding Axiom/Velox.

Usage: scripts/gen_third_party.py [--axiom-src DIR] [--axiom-build DIR]
"""

import argparse
import json
import os
import shlex
import shutil
import subprocess

# Axiom targets whose link lines together cover everything Ember links.
ANCHOR_TARGETS = [
    "axiom/optimizer/tests/axiom_optimizer_tests",
    "axiom/runner/tests/axiom_runner_local_runner_test",
    "axiom/logical_plan/tests/axiom_logical_plan_tests",
    "axiom/connectors/file/tests/axiom_file_connector_test",
]

# Source file whose compile command supplies include paths, defines and
# machine flags. Axiom, Velox and folly must all be compiled identically.
FLAGS_SOURCE = "axiom/runner/LocalRunner.cpp"

TEST_MARKERS = ("/tests/", "gtest", "gmock", "fuzzer")
# Ember tests use their own main (ember/tests/Main.cpp), which inits folly.
GTEST_MAIN = "libgtest_main.a"


def link_libraries(build_ninja, target):
    with open(build_ninja) as f:
        found = False
        for line in f:
            if line.startswith(f"build {target}:"):
                found = True
            elif found and line.strip().startswith("LINK_LIBRARIES"):
                return shlex.split(line.split("=", 1)[1])
    raise SystemExit(f"link line for {target} not found in {build_ninja}")


def absolutize(build_dir, item):
    if item.startswith("-") or os.path.isabs(item):
        return item
    return os.path.normpath(os.path.join(build_dir, item))


def compile_flags(build_dir, src_dir):
    with open(os.path.join(build_dir, "compile_commands.json")) as f:
        commands = json.load(f)
    target = os.path.realpath(os.path.join(src_dir, FLAGS_SOURCE))
    for entry in commands:
        if os.path.realpath(os.path.join(entry["directory"], entry["file"])) == target:
            args = shlex.split(entry["command"])[1:]
            directory = entry["directory"]
            break
    else:
        raise SystemExit(f"{FLAGS_SOURCE} not in compile_commands.json")

    flags = []
    it = iter(args)
    for arg in it:
        if arg in ("-isystem", "-I"):
            flags += [arg, os.path.normpath(os.path.join(directory, next(it)))]
        elif arg.startswith("-I"):
            flags.append("-I" + os.path.normpath(os.path.join(directory, arg[2:])))
        elif arg in ("-o", "-c", "-MF", "-MT"):
            next(it)
        elif arg.startswith(("-D", "-m", "-f", "-std=")):
            flags.append(arg)
    return flags


def fbthrift_link_libraries(velox_src, probe_build):
    probe_src = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                             "fbthrift_probe")
    subprocess.run(
        ["cmake", "-S", probe_src, "-B", probe_build, "-GNinja", "-Wno-dev",
         "-DCMAKE_BUILD_TYPE=Release",
         # Velox's find modules (Sodium, Xxhash, ...) that fbthrift's config
         # needs.
         f"-DCMAKE_MODULE_PATH={os.path.join(velox_src, 'CMake')}"],
        check=True, stdout=subprocess.DEVNULL)
    return [absolutize(probe_build, x) for x in link_libraries(
        os.path.join(probe_build, "build.ninja"), "fbthrift_probe")]


def python_flags(python):
    """Include flags for Python and pybind11, from the interpreter that runs
    Ember's Python package (python/). Empty without one."""
    if not (python and os.path.exists(python)):
        return []
    out = subprocess.run(
        [python, "-c",
         "import sysconfig, pybind11; "
         "print(sysconfig.get_paths()['include']); print(pybind11.get_include())"],
        check=True, capture_output=True, text=True).stdout.split()
    return [flag for path in out for flag in ("-isystem", path)]


def unique(items):
    seen = set()
    return [x for x in items if not (x in seen or seen.add(x))]


def main():
    parser = argparse.ArgumentParser()
    home = os.path.expanduser("~")
    parser.add_argument("--axiom-src", default=os.path.join(home, "source/axiom"))
    parser.add_argument("--axiom-build", default=None)
    parser.add_argument("--velox-src", default=os.path.join(home, "source/velox"))
    parser.add_argument(
        "--python",
        default=os.path.join(os.path.dirname(os.path.abspath(__file__)),
                             "..", ".venv", "bin", "python"),
        help="Python with pybind11 for the Python binding (python/)")
    args = parser.parse_args()
    src_dir = os.path.realpath(args.axiom_src)
    build_dir = os.path.realpath(
        args.axiom_build or os.path.join(src_dir, "_build/release"))

    libs = []
    for target in ANCHOR_TARGETS:
        libs += [absolutize(build_dir, x)
                 for x in link_libraries(os.path.join(build_dir, "build.ninja"), target)]
    libs = unique(libs)

    libs = [x for x in libs if not x.endswith(GTEST_MAIN)]
    test_libs = [x for x in libs
                 if any(m in x for m in TEST_MARKERS)]
    main_libs = [x for x in libs if x not in test_libs]

    def group(items):
        # One link group: static archives in CMake's order still have
        # circular references between Velox, Axiom and folly.
        return ["-Wl,--start-group"] + items + ["-Wl,--end-group"]

    out_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                           "..", "third-party", "generated")
    os.makedirs(out_dir, exist_ok=True)
    fbthrift_libs = unique(fbthrift_link_libraries(
        os.path.realpath(args.velox_src),
        os.path.join(build_dir, "fbthrift_probe")))
    thrift1 = shutil.which("thrift1")
    if thrift1 is None:
        raise SystemExit("thrift1 (fbthrift's compiler) not on PATH")
    with open(os.path.join(out_dir, "deps.bzl"), "w") as f:
        f.write("# Generated by scripts/gen_third_party.py. Do not edit.\n")
        f.write(f"# Axiom source: {src_dir}\n# Axiom build: {build_dir}\n\n")
        for name, value in [
            ("PREPROCESSOR_FLAGS", compile_flags(build_dir, src_dir)),
            ("LINKER_FLAGS", group(main_libs)),
            ("TEST_LINKER_FLAGS", group(test_libs)),
            ("FBTHRIFT_LINKER_FLAGS", group(fbthrift_libs)),
            ("THRIFT1", thrift1),
            ("PYTHON_PREPROCESSOR_FLAGS", python_flags(args.python)),
        ]:
            f.write(f"{name} = {json.dumps(value, indent=4)}\n\n")
    print(f"wrote {os.path.normpath(os.path.join(out_dir, 'deps.bzl'))}")


if __name__ == "__main__":
    main()
