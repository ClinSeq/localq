try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

# For making things look nice on pypi:
# try:
#     import pypandoc
#     long_description = pypandoc.convert('README.md', 'rst')
# except (IOError, ImportError):

long_description = 'Job Scheduler for a single server node'

setup(name='localq',
      version='0.3.7',
      description='Local Job scheduler',
      author='Daniel Klevebring',
      author_email='daniel.klevebring@gmail.com',
      url='http://github.com/dakl/localq',
      license='MIT License',
      install_requires=['Pyro4', 'networkx'],
      packages=[
          'localq'
      ],
      keywords=[
          'scheduler',
          'cluster',
          'load-balancer'
      ],
      scripts=[
          'scripts/localqserver_start',
          'scripts/localqserver_wait',
          'scripts/lbatch',
          'scripts/lqueue',
          'scripts/lcancel',
          'scripts/linfo'
      ],
      classifiers=[
          "Programming Language :: Python",
          "Programming Language :: Python :: 2.7",
          "License :: OSI Approved :: MIT License",
          "Development Status :: 3 - Alpha",
          "Operating System :: MacOS :: MacOS X",
          "Operating System :: Unix",
          "Intended Audience :: Science/Research",
          "Topic :: Scientific/Engineering :: Bio-Informatics",
      ],
      long_description=long_description,
      )
