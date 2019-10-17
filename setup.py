import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="hcacdm",
    version="0.0.3",
    author="hewgreen",
    author_email="hewgreen1@gmail.com",
    description="Metadata converter for HCA sequencing datasets to the atlas common data format.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/ebi-gene-expression-group/hca-cdm",
    packages=setuptools.find_packages(),
    install_requires=[
                'hca',
                'hca-ingest',
                'networkx',
                'requests',
                'urllib3'
          ],
    dependency_links=[
        'https://github.com/ebi-gene-expression-group/common-datamodel',
        'https://github.com/ebi-gene-expression-group/hca-cdm'
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6'
)
