import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    # TODO: rename to relops-worker-tools, must rename package dir also
    name="worker_health",
    version="0.0.1",
    author="Andrew Erickson",
    author_email="aerickson@mozilla.com",
    description="tools for working with workers",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/mozilla-platform-ops/android-tools",
    project_urls={
        "Bug Tracker": "https://github.com/mozilla-platform-ops/android-tools/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    package_dir={"": "."},
    packages=setuptools.find_packages(where="."),
    python_requires=">=3.6",
)
