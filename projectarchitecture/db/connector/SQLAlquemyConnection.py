from abc import ABCMeta, abstractmethod

import pandas as pd
from sqlalchemy import create_engine, text, update
from sqlalchemy.orm import sessionmaker
from tqdm import tqdm

from projectarchitecture.db.connector.AbstractORMConnection import AbstractORMConnection
from projectarchitecture.db.connector.dialects.CustomDialect import CustomDialect
from projectarchitecture.util.Constants import Constants
from projectarchitecture.util.LoggerUtils import LoggerUtils
from projectarchitecture.util.Singleton import Singleton
from projectarchitecture.util.YamlUtils import YamlUtils

logger = LoggerUtils.get_logger('AbstractSQLAlquemyConnection')

class AbstractSQLAlquemyConnection(AbstractORMConnection):

    __metaclass__ = ABCMeta

    def __init__(self, bucket, property_path):
        logger.info("Bucket {}, Property {}".format(bucket, property_path))
        self.properties = YamlUtils(bucket, property_path).properties
        self.connect()

    def connect(self):
        engine = create_engine(self.get_connection_url(self.properties))
        self.connection = engine.connect()
        Session = sessionmaker()
        self.session = Session(bind=self.connection)

    def disconnect(self):
        self.session.close()

    def find(self, query, table_clause, params = None):
        query_obj = self.get_session().query(table_clause).from_statement(text(query))
        if params is not None:
            query_obj = query_obj.params(params)
        return query_obj.all()

    def insert(self, entity, commit = False):
        self.get_session().add(entity)
        if commit:
            self.get_session().commit()

    def insert_many(self, entities, commit = False):
        self.get_session().add_all(entities)
        if commit:
            self.get_session().commit()

    def delete(self, entity, commit = False):
        self.get_session().delete(entity)
        if commit:
            self.get_session().commit()

    def update(self, table_clause, where, values, commit = False):
        self.get_session().query(table_clause).filter_by(**where).update(dict(**values))
        if commit:
            self.get_session().commit()

    def __get_update_str_list(self, table_clause, wheres_values):
        query_list = []
        for where_value in wheres_values:
            stmt = update(table_clause).where(where_value['wheres']).values(**where_value['values'])
            query_list.append(str(stmt.compile(dialect=CustomDialect(), compile_kwargs={"literal_binds": True})))
        return query_list

    def __execute_querys_iterativily(self, querys, commit):
        count = 0
        querys_str = ""
        for query in tqdm(querys):
            querys_str = querys_str + query + ";"
            if count >= Constants.PAGINATE_QUERYS:
                self.execute_query(querys_str, commit=commit)
                count = 0
                querys_str = ""
            else:
                count = count + 1
        if querys_str != "":
            self.execute_query(querys_str, commit=commit)

    def update_bulk(self, table_clause, wheres_values, commit = False):
        querys = self.__get_update_str_list(table_clause, wheres_values)
        self.__execute_querys_iterativily(querys, commit)

    def commit(self):
        self.session.commit()

    def rollback(self):
        self.session.rollback()

    def get_session(self):
        return self.session

    def execute_query(self, query, commit = False):
        conn = self.get_session().connection().connection
        cursor = conn.cursor()
        [cursor for cursor in cursor.execute(query, multi=True)]
        if commit:
            conn.commit()

    @abstractmethod
    def get_connection_url(self, properties):
        pass

"""
Es necesario instalar módulo mysqlclient
pip install mysql-connector-python-rf
"""
@Singleton
class MySQLSQLAlquemyConnection(AbstractSQLAlquemyConnection):

    def get_connection_url(self, properties):
        return "mysql+mysqlconnector://{}:{}@{}:{}/{}".format(self.properties["mysql"]["user"],
                                            self.properties["mysql"]["password"],
                                            self.properties["mysql"]["endpoint"],
                                            self.properties["mysql"]["port"],
                                            self.properties["mysql"]["database"])


"""
Es necesario instalar módulo mysqlclient
pip install sqlalchemy-redshift
"""
@Singleton
class RedshiftSQLAlquemyConnection(AbstractSQLAlquemyConnection):

    def connect(self):
        engine = create_engine(self.get_connection_url(self.properties), connect_args={'sslmode': 'prefer'})
        self.connection = engine.connect()
        Session = sessionmaker()
        self.session = Session(bind=self.connection)

    def get_connection_url(self, properties):
        return "redshift+psycopg2://{}:{}@{}:{}/{}".format(self.properties["redshift"]["user"],
                                            self.properties["redshift"]["password"],
                                            self.properties["redshift"]["endpoint"],
                                            self.properties["redshift"]["port"],
                                            self.properties["redshift"]["database"])


if  __name__ == "__main__":

    connection = MySQLSQLAlquemyConnection.instance(Constants.BUCKET, Constants.PROPERTY_PATH)
    df = pd.read_sql_query("select * from ninjaproject.user", connection.connection)
    print(df)

    #where_value = {'values': {'management_fee_applied_reference_date': '2008-09-08', 'domicile_of_the_custodian': 'LU', 'performance_fee_applied': 0.0, 'ongoing_charges_date': '2018-08-31'}, 'wheres': FundInfoEntity.id_fund == 1}
    #stmt = update(FundInfoEntity).where(where_value['wheres']).values(**where_value['values'])
    #print(str(stmt.compile(dialect=CustomDialect(), compile_kwargs={"literal_binds": True})))


    #connection = MySQLHelenaSQLAlquemyConnection.instance("universo-bbg", "conf/properties-mifid-local.yml")

    #df = pd.read_sql_query("select * from mifid.t_master_ept where id_fund = 999999", connection.connection)

    """time_str = str(datetime.now(gettz("Europe/Madrid")))
    object = AlternativeIdEntity(id_fund=999999, cod_isin="JHR", des_system="1", date_creation=time_str)
    connection.insert(object, commit=True)"""

    """connection.execute_query("delete from mifid.t_master_ept where id_fund = 999999", commit=True)

    object = MasterEptEntity(id_fund = 999999, cod_isin = "JHR", qty_observer_excess_kurtosis = Decimal(57.985800))
    connection.insert(object, commit=True)
    print(object.qty_observer_excess_kurtosis)"""


    """time_str = str(datetime.now(gettz("Europe/Madrid")))
    object = AlternativeIdEntity(id_fund=1, cod_isin="2", des_system="1", date_creation = time_str)
    connection.insert(object, commit=True)"""
    #print(connection.get_session().query(func.max(MasterEmtEntity.id_fund)).scalar() + 1)

    #connection.execute_query("delete from mifid.t_historic_emt where date = '2017-12-31'", commit=True)

    """print(connection.get_session().query(MasterEmtEntity).filter(HistoricEmtEntity.date.in_(["2018-01-01"])).delete(synchronize_session=False))"""

    """print(connection.get_session().query(MasterEmtEntity).count())
    print(connection.find("select * from mifid.t_master_emt", MasterEmtEntity))
    print(connection.find("select * from mifid.t_master_emt where cod_isin=:id", MasterEmtEntity, {"id": 1}))"""


    """stmt = update(MasterEmtEntity).where(MasterEmtEntity.cod_isin=="CH0002795703").values(cod_currency='COD')
    print(stmt.compile(dialect=mysql.dialect(), compile_kwargs={"literal_binds": True}))

    connection.update_bulk(MasterEmtEntity, [{"wheres": MasterEmtEntity.cod_isin=="CH0002795703", "values" : {"cod_currency": "111"}},
                                             {"wheres": MasterEmtEntity.cod_isin == "CH0002795729","values": {"cod_currency": "222"}}], commit=True)"""
    #connection.execute_query(stmt, commit=True)
    #connection.connection.execute(stmt)

    """connection.get_session().query(MasterEmtEntity).filter_by(cod_isin="CH0002795703").update(dict(cod_currency="USD"))
    connection.get_session().commit()"""

    #connection.update(MasterEmtEntity, {"cod_isin": "7"}, {"cod_currency": "QQQ"}, commit=True)
    #connection.update(MasterEmtEntity, MasterEmtEntity.cod_isin=="7", cod_currency="EUR")




    #print(connection.find("select * from t_master_emt", MasterEmtEntity))
    #connection.get_session().query(MasterEmtEntity).all()
    #print(connection.find("select * from t_historic_emt", HistoricEmtEntity))
    """print(connection.find("select * from t_historic_emt", MasterEmtEntity))"""
    """print(connection.find("select * from t_master_emt where cod_isin=:id", MasterEmtEntity, {"id": 1}))
    print(connection.find("select * from t_master_emt where cod_isin=:id", MasterEmtEntity, {"id": 2}))

    emt_object = connection.find("select * from t_master_emt where cod_isin=:id", MasterEmtEntity, {"id": 1})[0]
    emt_object.qty_distribution_fee = 7;
    connection.update(emt_object, commit=True);"""

