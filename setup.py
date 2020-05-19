import setuptools

setuptools.setup(
    name='conductor',
    version='0.1.0',
    author='Nicholas Leo Martin',
    author_email='nickleomartin@gmail.com',
    description='A singer.io module to orchestrate tap-to-target replication',
    py_modules=['conductor'],
    classifiers=[
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3 :: Only'
    ],
    install_requires=[],
    extras_require={
        'smart': ['smart-open==2.0.0']
    },
    entry_points="""
          [console_scripts]
          conductor=conductor:main
      """,
    packages=['conductor'],
    package_data={},
    include_package_data=True,
)
