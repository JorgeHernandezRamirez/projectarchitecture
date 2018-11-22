from setuptools import setup

setup(
    name='projectarchitecture',
    version='1.1.0-SNAPSHOT',
    packages=["projectarchitecture",
              "projectarchitecture/db/connector",
              "projectarchitecture/db/connector/dialects",
              "projectarchitecture/db/connector/factory",
              "projectarchitecture/db/util",
              "projectarchitecture/util"],
    install_requires=['pandas==0.21.0',
                      'sqlalchemy==1.2.2',
                      'mysql-connector-python-rf',
                      'pyyaml',
                      'tqdm',
                      'boto3']
)
