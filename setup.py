from setuptools import setup, find_packages

version = '0.0.1.dev1'
long_description = """
Retrieve data through YNHH API
"""

setup(
	name='api_retriever',
	version=version,
	description='package for accessing YNHH API',
	long_description=long_description,
	author='Clinton Wang',
	author_email='clintonjwang@gmail.com',
	packages=find_packages(exclude=['contrib', 'docs', 'tests*']),
	url='https://github.com/clintonjwang/api-queries'
)
