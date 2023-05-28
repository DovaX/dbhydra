import setuptools
    
with open("README.md", "r") as fh:
    long_description = fh.read()
    
setuptools.setup(
    name='dbhydra',
    version='1.0.0',
    author='DovaX',
    author_email='dovax.ai@gmail.com',
    description='Data science friendly ORM combining Python',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/DovaX/dbhydra',
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=[
          'pyodbc','pandas','pymysql','pymongo','google-cloud-bigquery'
     ],
    python_requires='>=3.6',
)
    