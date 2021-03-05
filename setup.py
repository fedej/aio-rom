from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="aio-rom",
    version="0.0.3",
    author="Federico Jaite",
    author_email="fede_654_87@hotmail.com",
    description="asyncio based Redis object mapper",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/fedej/aio-rom",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Framework :: AsyncIO",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.7",
        "Topic :: Database :: Front-Ends",
    ],
    python_requires='>=3.7',
    install_requires=[
        "aioredis>=1.3.1",
        "typing-inspect>=0.6.0"
    ],
)
