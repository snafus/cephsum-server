#from setuptools import setup
from setuptools import setup, find_packages

pkg_vars  = {}
with open("cephsumserver/_version.py") as fp:
    exec(fp.read(), pkg_vars)

setup(name='cephsumserver',
      version=pkg_vars['__version__'],
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

