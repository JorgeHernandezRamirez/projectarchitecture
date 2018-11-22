from abc import ABCMeta, abstractmethod


class AbstractORMConnection:

    __metaclass__ = ABCMeta

    @abstractmethod
    def connect(self):
        """
        Método que debe conectarse a la base de datos
        :return:
        """
        pass

    @abstractmethod
    def disconnect(self):
        """
        Método que debe desconectarse de la bd
        :return:
        """
        pass

    def find(self, query, table_clause):
        """
        Método que debe buscar en la base de datos
        :param id:
        :param table_clause:
        :return:
        """
        pass

    @abstractmethod
    def insert(self, entity, commit = False):
        """
        Método que debe insertar la entidad en base de datos
        :param entity:
        :return:
        """
        pass

    @abstractmethod
    def delete(self, entity, commit = False):
        """
        Método que debe borrar la entidad de base de datos
        :param entity:
        :param table_clause:
        :return:
        """
        pass

    @abstractmethod
    def update(self, entity, commit = False):
        """
        Método que debe actualizar la entidad
        :param entity_id:
        :param entity:
        :param table_clause:
        :return:
        """
        pass

    @abstractmethod
    def commit(self):
        """
        Método que debe realizar el commit de la bd
        :return:
        """
        pass

    @abstractmethod
    def rollback(self):
        """
        Método que debe realizar el rollback de la bd
        :return:
        """
        pass

    @abstractmethod
    def get_session(self):
        """
        Método que tiene que devolver la sesión del objeto conecction
        :return:
        """
        pass

    @abstractmethod
    def execute_query(self, query, commit = False):
        """
        Método que ejecuta una query
        :param query:
        :return:
        """
        pass