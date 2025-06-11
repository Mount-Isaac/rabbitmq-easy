from setuptools import setup, find_packages
import os

# Read the contents of README file
this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

# Read requirements
with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name="rabbitmq-easy",
    version="1.0.3",
    author="Isaac Kyalo",
    author_email="isadechair019@gmail.com",
    description="A simple, robust RabbitMQ manager for Python applications",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/mount-isaac/rabbitmq-easy",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: System :: Networking",
        "Topic :: Internet",
    ],
    python_requires=">=3.7",
    install_requires=requirements,
    extras_require={
        'dev': [
            'pytest>=6.0',
            'pytest-cov',
            'black',
            'flake8',
            'mypy',
            'twine',
            'wheel',
        ],
    },
    keywords="rabbitmq amqp queue messaging",
    project_urls={
        "Bug Reports": "https://github.com/yourusername/rabbitmq-easy/issues",
        "Source": "https://github.com/yourusername/rabbitmq-easy",
        "Documentation": "https://github.com/yourusername/rabbitmq-easy#readme",
    },
)
