# Include setup.py to enable --editable installs.
import setuptools


INSTALL_REQUIRES = [
    "boto3",
    "paramiko",
    "pyftpdlib",
    "pytz",
]


setuptools.setup(
    name="filesystem",
    version="1.0",
    description=(
        "FileSystem provides a basic file system abstraction "
        "with multiple back-end implementations"
    ),
    author="Alexandros Tzannes and Braxton Mckee",
    author_email="atzannes@gmail.com and braxton.mckee@gmail.com",
    url="https://github.com/aprioriinvestments/filesystem",
    packages=setuptools.find_packages(),
    install_requires=INSTALL_REQUIRES,
    # https://pypi.org/classifiers/
    classifiers=[
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3 :: Only",
    ],
    license="Apache Software License v2.0",
)
