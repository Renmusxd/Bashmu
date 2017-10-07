#!/usr/bin/env python

from distutils.core import setup

setup(name='bashmu',
      version='0.1',
      description='A Pretty Python Distributed Computing Framework',
      author='Sumner Hearth',
      author_email='sumnernh@gmail.com',
      url='https://github.com/Renmusxd/Bashmu',
      packages=['bashmu'],
      keywords="pretty decorator distributed computing",
      license="MIT",
      install_requires=["dill"]
      )
