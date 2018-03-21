from setuptools import setup, find_packages
import sys, os.path

# Don't import gym module here, since deps may not be installed
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'theDoc'))


setup(
    name="theDoc",
    version="0.1",
    packages=[package for package in find_packages() if package.startswith('theDoc')],
)
