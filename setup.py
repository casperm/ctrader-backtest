from setuptools import setup


setup(
    name='tdbc34',
    version='1.0.1',    
    description='Read cTrader local cached data',
    url='https://github.com/casperm/ctrader-backtest',
    author='Casper Mak',
    author_email='ask@casper.me.uk',
    license='MIT License',
    packages=['tdbc34'],
    install_requires=[
        'pandas>=1.5.0',
        'pyarrow>=9.0.0'
    ],
)
