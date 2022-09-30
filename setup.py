from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf8") as fh:
    long_description = fh.read()

setup(
    name="brickflow",
    author="Sri Tikkireddy",
    author_email="sri.tikkireddy@databricks.com",
    description="Databricks Brickflow",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="",
    license="",
    packages=find_packages(include=["*"], exclude=['tests', 'tests.*', ]),
    use_scm_version={
        "local_scheme": "dirty-tag"
    },
    setup_requires=['setuptools_scm'],
    install_requires=[
        'cdktf>=0.12.2',
        'setuptools>=45',
        'python-decouple>=3.6'
    ] ,
    package_data={'': ['databricks_databricks@0.0.0.jsii.tgz', 'py.typed']},
    entry_points='''
        [console_scripts]
    ''',
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)