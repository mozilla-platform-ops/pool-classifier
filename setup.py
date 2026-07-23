import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    # TODO: rename package dir from worker_health to pool_classifier.
    name="worker_health",
    version="1.0.0",
    author="Andrew Erickson",
    author_email="aerickson@mozilla.com",
    description="Taskcluster pool classifier service and dashboard",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/mozilla-platform-ops/pool-classifier",
    project_urls={
        "Bug Tracker": "https://github.com/mozilla-platform-ops/pool-classifier/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.11",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    package_dir={"": "."},
    packages=setuptools.find_packages(where="."),
    python_requires=">=3.11",
)
