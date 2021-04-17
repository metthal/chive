from setuptools import setup

setup(
    name="chives",
    version="0.1.0",
    packages=["chives"],
    install_requires=[
        "aio-pika~=6.8.0",
        "ujson~=4.0.2"
    ],
    extras_require={
        "redis": [
            "aioredis~=1.3.1"
        ],
        "dev": [
            "mypy>=0.812,<1.0",
            "pytest~=6.2.3",
            "pytest-asyncio~=0.14.0",
            "pytest-docker-compose~=3.2.1",
            "pytest-sugar~=0.9.4",
        ],
    },
    entry_points={
        #    "console_scripts": ["chives=chives:main"]
    },
)
