from setuptools import setup, find_packages
import sys, os.path

# Don't import theDoc module here, since deps may not be installed
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'theDoc'))
from version import VERSION

setup(
    name="theDoc",
    version=VERSION,
    description="theDoc; a machine learning- powered MLB game prediction and betting engine. Code named for the original author's favourite MLB player Roy 'Doc' Halladay.",
    author='Dan Goldberg',
    author_email='dgoldberg48@gmail.com',
    license='',
    packages=[package for package in find_packages() if package.startswith('theDoc')],
    install_requires=[
        'numpy','pandas','matplotlib','beautifulsoup4','tensorflow-gpu','keras','scikit-learn','scipy','python-forecastio'
    ],
    extras_require={
        'notebooks':['jupyterlab']
    },
    tests_require=['pytest','mock']
)