from setuptools import setup

setup(name='cephsumserver',
      version='0.0.1',
      description='Server based cephsum code',
      url='http://github.com/storborg/funniest',
      author='james.walder',
      author_email='james.walder@NOSPAM.ac.uk',
      license='MIT',
      packages=['cephsumserver'],
      entry_points = {
        'console_scripts': ['cephserve=scripts.cephserver:main'],
      },
      zip_safe=False)

