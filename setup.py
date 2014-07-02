import re
import setuptools
from setuptools.command.test import test as TestCommand


class PyTest(TestCommand):

    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = ['tests.py']
        self.test_suite = True
    def run_tests(self):
        import pytest
        pytest.main(self.test_args)


install_requires = [
    'pilo >=0.3.2,<0.4',
    'SQLAlchemy >=0.9,<0.10',
]

extras_require = {
    'tests': [
        'pytest >=2.5.2,<3',
        'pytest-cov >=1.7,<2',
        'mock >=1.0,<2.0',
        'unittest2 >=0.5.1,<0.6',
        'psycopg2 >=2.5,<3.0',
    ],
}

setuptools.setup(
    name='sqlalchemy-pilo',
    version=(
        re
        .compile(r".*__version__ = '(.*?)'", re.S)
        .match(open('sqlalchemy_pilo.py').read())
        .group(1)
    ),
    url='https://github.com/bninja/sqlalchemy-pilo/',
    license=open('LICENSE').read(),
    author='egon',
    author_email='spengler@gb.com',
    description='Mongo.',
    long_description=open('README.rst').read(),
    py_modules=['sqlalchemy_pilo'],
    include_package_data=True,
    extras_require=extras_require,
    tests_require=extras_require['tests'],
    install_requires=install_requires,
    cmdclass={'test': PyTest},
    classifiers=[
        'Intended Audience :: Developers',
        'Development Status :: 4 - Beta',
        'Natural Language :: English',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'License :: OSI Approved :: ISC License (ISCL)',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
    ],
)
