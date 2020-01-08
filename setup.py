import setuptools

with open('README.md', 'r') as fh:
    long_description = fh.read()

from pgreplicaauditor import VERSION

setuptools.setup(
    name='pg-replica-auditor',
    version=VERSION,
    author='Lev Kokotov',
    author_email='lev.kokotov@instacart.com',
    description="Run a few sanity checks on your logical PostgreSQL replica to make sure its the same as the primary.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/levkk/pg-replica-auditor',
    install_requires=[
        'Click>=7.0',
        'colorama>=0.4.3',
        'psycopg2>=2.8.4',
        'tqdm>=4.41.1'
    ],
    packages=setuptools.find_packages(),
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent', # Colorama!
    ],
    python_requires='>=3',
    entry_points={
        'console_scripts': [
            'pgreplicaauditor = checksummer:main',
        ]
    },
)