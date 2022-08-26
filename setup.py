from setuptools import setup, find_packages

setup(
    name="debussy_airflow",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "apache-airflow-providers-google==8.0.0",
        "facebook-business==13.0.0",
        "mysql-connector-python==8.0.24",
        "paramiko==2.8.1"
    ]
)
