# setup.py
from setuptools import setup, find_packages

setup(
    name='your_project',
    version='1.0.0',
    packages=find_packages(),
    install_requires=[
        # 'apache-beam[gcp]',
        'setuptools==57.5.0',
        'functions-framework==3.*'
        # Add any other dependencies here
    ],
)
