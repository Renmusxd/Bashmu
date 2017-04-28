#!/usr/bin/env python

from distutils.core import setup
from Cython.Build import cythonize

setup(name='Bashmu',
      version='0.1',
      description='A Pretty Python Distributed Computing Framework',
      author='Sumner Hearth',
      author_email='sumnernh@gmail.com',
      url='https://github.com/Renmusxd/Bashmu',
      packages=['Bashmu'],
      keywords="pretty decorator distributed computing",
      license="MIT",
      install_requires=["dill"],
      ext_modules=cythonize("ext/FormatSock.pyx")
      )
