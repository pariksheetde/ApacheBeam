import setuptools

setuptools.setup(
    name = "my-package",
    version="0.1",
    install_requires=[
        "apache_beam[gcp]==2.53.0",
        "google_cloud-storage",
        "google_cloud-logging"
    ],
    packages=setuptools.find_packages(),
)