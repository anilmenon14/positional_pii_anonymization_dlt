from setuptools import setup, find_packages

setup(
    name='pii_analyze_anonymize',
    version='0.1',
    packages=find_packages(),
    author='Anil Menon',
    author_email='anil.menon@databricks.com',
    description='A package for using Presidio analyze and anonymize functions within data pipelines',
    classifiers=[
        'Programming Language :: Python :: 3',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',
)
