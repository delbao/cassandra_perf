
from __future__ import print_function
import os
import sys

from setuptools import setup
from setuptools import find_packages

from distutils.cmd import Command

long_description = ""
with open("README.rst") as f:
    long_description = f.read()


class DocCommand(Command):

    description = "generate or test documentation"

    user_options = [("test", "t",
                     "run doctests instead of generating documentation")]

    boolean_options = ["test"]

    def initialize_options(self):
        self.test = False

    def finalize_options(self):
        pass

    def run(self):
        if self.test:
            path = "docs/_build/doctest"
            mode = "doctest"
        else:
            path = "docs/_build/%s" % __version__
            mode = "html"

            try:
                os.makedirs(path)
            except:
                pass

        if has_subprocess:
            # Prevent run with in-place extensions because cython-generated objects do not carry docstrings
            # http://docs.cython.org/src/userguide/special_methods.html#docstrings
            import glob
            for f in glob.glob("cassandra/*.so"):
                print("Removing '%s' to allow docs to run on pure python modules." %(f,))
                os.unlink(f)

            # Build io extension to make import and docstrings work
            try:
                output = subprocess.check_output(
                    ["python", "setup.py", "build_ext", "--inplace", "--force", "--no-murmur3", "--no-cython"],
                    stderr=subprocess.STDOUT)
            except subprocess.CalledProcessError as exc:
                raise RuntimeError("Documentation step '%s' failed: %s: %s" % ("build_ext", exc, exc.output))
            else:
                print(output)

            try:
                output = subprocess.check_output(
                    ["sphinx-build", "-b", mode, "docs", path],
                    stderr=subprocess.STDOUT)
            except subprocess.CalledProcessError as exc:
                raise RuntimeError("Documentation step '%s' failed: %s: %s" % (mode, exc, exc.output))
            else:
                print(output)

            print("")
            print("Documentation step '%s' performed, results here:" % mode)
            print("   file://%s/%s/index.html" % (os.path.dirname(os.path.realpath(__file__)), path))


from cassandra_perf import __version__


def run_setup():

    dependencies = ['six >=1.6',
                    'scales',
                    'cassandra-driver']

    PY3 = sys.version_info[0] == 3
    if not PY3:
        dependencies.append('futures')

    setup(
        name='cassandra_perf',
        version=__version__,
        description='Cassandra Python Perf Test Framework',
        long_description=long_description,
        url='https://github.com/delbao/cassandra_perf',
        author='Del Bao',
        author_email='dbao@dont-reply.com',
        packages=find_packages(exclude=['tests*']),
        keywords='cassandra,perf',
        include_package_data=True,
        install_requires=dependencies,
        tests_require=['mock<=1.0.1', 'PyYAML', 'pytz', 'sure'],
    )

run_setup()