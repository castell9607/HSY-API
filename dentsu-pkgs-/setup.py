import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="dentsu-pkgs",
    version="0.3.0",
    author="dentsu data labs",
    author_email="oscar.castellon@dentsu.com",
    description="A python library of helper functions for dentsu's cloud solutions.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/castell9607/dentsu-pkgs",
    packages=setuptools.find_packages(),
    classifiers=["Programming Language :: Python :: 3", "License :: OSI Approved :: MIT License", "Operating System :: OS Independent",],
    python_requires=">=3.6",
)
