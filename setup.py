from setuptools import setup, find_packages

# read the contents of your README file
from os import path
this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name="debussy_airflow",
    version="0.0.3",
    author="Lawrence Fernandes",
    author_email="lawrence.stfs@gmail.com",
    description="Debussy Concert provider for Airflow",
    long_description=long_description,
    long_description_content_type='text/markdown',
    license="Apache Software License (http://www.apache.org/licenses/LICENSE-2.0)",
    url='https://github.com/DotzInc/debussy_airflow',
    packages=find_packages('debussy_airflow'),
    package_dir={'': 'debussy_airflow'},
    install_requires=[
        "apache-airflow-providers-google==8.0.0",
        "facebook-business==13.0.0",
        "mysql-connector-python==8.0.24",
        "paramiko==2.8.1"
    ],
    classifiers=[
        'Programming Language :: Python :: 3',
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7'
)
