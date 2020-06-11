from setuptools import setup, find_packages
import os
import sys

_here = os.path.abspath(os.path.dirname(__file__))
version = {}

with open(os.path.join(_here, 'ingresspipe', 'version.py')) as f:
    exec(f.read(), version)

if sys.version_info[0] < 3:
    with open(os.path.join(_here, 'README.md')) as f:
        long_description = f.read()
else:
    with open(os.path.join(_here, 'README.md'), encoding='utf-8') as f:
        long_description = f.read()

setup(
    
    name='ingresspipe',
    version=version['__version__'],
    description='Packages responsible for data ingress from HTAN collaborators',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='Milen Nikolov',
    author_email='milen.nikolov@sagebase.org',
    url='https://github.com/Sage-Bionetworks/HTAN-data-pipeline/tree/organized-into-packages',
    packages=find_packages(include=['ingresspipe', 'ingresspipe.*']),
    python_requires='>=3.0',
    install_requires=[
        'networkx==2.2', 'rdflib==4.2.2', 'tabletext==0.1', 'graphviz==0.8.4',
        'jsonschema==3.2.0', 'fastjsonschema==2.14.4', 'orderedset==2.0.1',
        'google-api-python-client==1.7.9', 'google-auth-httplib2==0.0.3', 'google-auth-oauthlib==0.4.0', 
        'pandas', 'pygsheets>=2.0.1', 'inflection==0.3.1', 'synapseclient>=1.9.2', 'setuptools>=41.2.0'
    ]
)