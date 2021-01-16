from setuptools import setup, find_packages

setup(
    name="aio-rom",
    version="0.0.1",
    description="asyncio based Redis object mapper",
    author="Federico Jaite",
    author_email="fede_654_87@hotmail.com",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    install_requires=[
        "aioredis>=1.3.1",
        "typing-inspect>=0.6.0"
    ],
)
