import setuptools
from setuptools import find_packages

with open("small.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name='etl-demo',
    version='0.0.3',
    author='Surya Kosana',
    author_email='spkosana.82@gmail.com',
    description='Testing installation of Package',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/spkosana/etl-demo.git',
    license='SKosana',
    packages=['etl-demo'],
    install_requires=['pandas'],
)
