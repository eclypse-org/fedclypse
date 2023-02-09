import setuptools, codecs, os

with open("README.rst", "r") as fh:
    long_description = fh.read()


def read(rel_path):
    here = os.path.abspath(os.path.dirname(__file__))
    with codecs.open(os.path.join(here, rel_path), "r") as fp:
        return fp.read()


def get_version(rel_path):
    for line in read(rel_path).splitlines():
        if line.startswith("__version__"):
            delim = '"' if '"' in line else "'"
            return line.split(delim)[1]
    else:
        raise RuntimeError("Unable to find version string.")


setuptools.setup(
    name="fedray",
    version=get_version("fedray/__init__.py"),
    author="Valerio De Caro",
    author_email="valerio.decaro@phd.unipi.it",
    description="FedRay: a Research Framework for Federated Learning based on Ray",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/vdecaro/fedray",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7,<3.11",
    install_requires=[
        "ray[tune]",
        "networkx",
    ],
    include_package_data=True,
)
