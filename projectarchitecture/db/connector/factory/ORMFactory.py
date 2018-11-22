import os

from projectarchitecture.db.connector.SQLAlquemyConnection import MySQLSQLAlquemyConnection, \
    RedshiftSQLAlquemyConnection
from projectarchitecture.util.Constants import Constants


class ORMFactory:

    def __init__(self, property_path):
        self._properties = property_path

    def __get_bucket_name(self):
        try:
            return os.environ['BUCKETNAME']
        except KeyError:
            return Constants.BUCKET

    def get_orm_connection(self):
        return MySQLSQLAlquemyConnection.instance(self.__get_bucket_name(), self._properties)

    def get_redshift_orm_connection(self):
        return RedshiftSQLAlquemyConnection.instance(self.__get_bucket_name(), self._properties)