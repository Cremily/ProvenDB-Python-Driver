import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pyproven", # Replace with your own username
    version="0.0.2",
    author="Mackenzie Harrison",
    author_email="mackenzieharrison97@gmail.com",
    description="A Python wrapper for ProvenSDB ",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Cremily/pyproven",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU Affero General Public License v3",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
