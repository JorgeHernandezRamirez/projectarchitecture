import boto3
import os
import yaml


class YamlUtils:

    DEFAULT_BUCKET = "universo-bbg"

    PROPERTIES_NAME_DEFAULT = "conf/properties.yml"

    REGION_NAME = "eu-west-1"

    def __init__(self, bucket = None, property_path = None):
        self._bucket = boto3.resource('s3', region_name=self.REGION_NAME).Bucket(self.__get_bucket(bucket))
        self._properties = self.__get_yaml_properties(property_path)

    def __get_bucket(self, bucket):
        return self.get_bucket_name() if bucket == None else bucket

    def __get_property_path(self, property_path):
        return self.PROPERTIES_NAME_DEFAULT if property_path == None else property_path

    def __get_yaml_properties(self, property_path):
        content_property = self._bucket.Object(self.__get_property_path(property_path)).get()['Body']
        return yaml.load(content_property)

    @property
    def properties(self):
        return self._properties

    def __getitem__(self, item):
        return self.properties[item]

    def get_bucket_name(self):
        try:
            return os.environ['BUCKETNAME']
        except KeyError as keyerror:
            return self.DEFAULT_BUCKET

    def get_property_path(self):
        try:
            return os.environ['PROPERTY_PATH']
        except KeyError as keyerror:
            return self.PROPERTIES_NAME_DEFAULT