#from setuptools import setup
from setuptools import setup, find_packages

setup(name='cephsumserver',
      version='0.0.2',
      description='Server based cephsum code',
      url='https://github.com/snafus/cephsum-server',
      author='james.walder',
      author_email='james.walder@NOSPAM.ac.uk',
      license='MIT',
      packages=find_packages(),
      #packages=['cephsumserver','cephsumserver.scripts'],
      #py_modules=['cephsumserver'],
      entry_points = {
        'console_scripts': ['cephserve=cephsumserver.scripts.cephserver:main'],
      },
      zip_safe=False)

