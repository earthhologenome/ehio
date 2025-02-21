from setuptools import setup, find_packages

setup(
    name="ehio",
    version="1.0.0",
    author="Raphael Eisenhofer, Antton Alberdi",
    author_email="antton.alberdi@sund.ku.dk",
    description="Input-output of EHI data between ERDA, Mjolnir and Airtable",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "numpy",
        "pandas",
        "argparse",
        "PyYAML"
    ],
    entry_points={
        "console_scripts": [
            "ehio=ehio.cli:main",
        ],
    },
    python_requires=">=3.6",
)
