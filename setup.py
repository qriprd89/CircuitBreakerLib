from setuptools import setup, find_packages

setup(
    name='CircuitBreakerLib',
    version='0.1.0',
    packages=find_packages(),  
    include_package_data=True,
    install_requires=[
        'PyYAML',
    ],
    author='Achal Bante',
    url='https://github.com/qriprd89/CircuitBreakerLib'
)