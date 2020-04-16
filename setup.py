from setuptools import setup

setup(
    name='twivents',
    version='0.1.0',
    description='',
    url='https://github.com/othompson2/twitter-events',
    author='Oliver Thompson',
    author_email='othompson2@sheffield.ac.uk',
    license='MIT',
    packages=['twivents'],
    entry_points={
        'console_scripts': [
            'twivents = twivents.__main__:main'
        ]
    },
    # production env
    install_requires=[
        'configargparse',
        'nltk',
        'numpy',
        'gensim',
        'cython',
        'pytz',
        'tweepy'
    ]
)
