import setuptools

NAME="vueflux"
VERSION="0.0.1"

with open("requirements.txt", "r") as _file:
    requirements = list(_file.readlines())

setuptools.setup(
    name=NAME,
    version=VERSION,
    author="Hayden Andreyka",
    author_email="haydenandreyka@gmail.com",
    description="An InfluxDB data source for Emporia Vue",
    install_requires=requirements,
    packages=setuptools.find_packages(),
)
