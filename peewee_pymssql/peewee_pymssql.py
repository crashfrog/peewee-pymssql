
from peewee import *
from peewee import logger, ColumnMetadata, ForeignKeyMetadata, IndexMetadata
import pymssql
from playhouse.db_url import connect, register_database
import playhouse.reflection

class MssqlDatabase(Database):

    field_overrides = {
        'bool': 'tinyint',
        'double': 'float(53)',
        'float': 'float',
        'int': 'int',
        'string': 'nvarchar',
        'fixed_char': 'nchar',
        'text': 'nvarchar(max)',
        'blob': 'varbinary',
        'uuid': 'nchar(40)',
        'primary_key': 'int identity',
        'datetime': 'datetime2',
        'date': 'date',
        'time': 'time',
    }

    def init(self, database, host, user, password):
        self.deferred = False
        self.database = database
        self.host = host
        self.user = user
        self.password = password
        self.log = logger.getChild(self.__class__.__name__)
        self._metadata_cache = {}

    def _connect(self):
        return pymssql.connect(database=self.database, host=self.host, user=self.user, password=self.password)

    def conflict_statement(self, on_conflict, query):
        raise NotImplementedError

    def conflict_update(self, on_conflict, query):
        raise NotImplementedError

    def get_indexes(self, table, schema='dbo'):
        return [IndexMetadata(name, "", name, True, table) for name, *_, con in self.md_cache(table, schema) if con == 'UNIQUE' or con == 'PRIMARY KEY']

    def md_cache(self, table, schema='dbo'):
        query = """
SELECT  
	C.COLUMN_NAME,
	C.COLUMN_DEFAULT,
	C.IS_NULLABLE,
	C.DATA_TYPE,
	C.CHARACTER_MAXIMUM_LENGTH,
	F.UNIQUE_CONSTRAINT_NAME,
	PP.TABLE_NAME,
	PP.COLUMN_NAME,
	T.CONSTRAINT_TYPE
     FROM
	INFORMATION_SCHEMA.COLUMNS C
	LEFT JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE U
	ON U.COLUMN_NAME = C.COLUMN_NAME AND U.TABLE_NAME = C.TABLE_NAME
	LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS T 
	ON T.CONSTRAINT_NAME = U.CONSTRAINT_NAME
	LEFT JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS F
	ON T.CONSTRAINT_NAME = F.CONSTRAINT_NAME
	LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS P
	ON F.UNIQUE_CONSTRAINT_NAME = P.CONSTRAINT_NAME
	LEFT JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE PP
	ON P.CONSTRAINT_NAME = PP.CONSTRAINT_NAME
	WHERE C.TABLE_NAME = %s
	AND C.TABLE_SCHEMA = %s
	ORDER BY U.CONSTRAINT_NAME desc;
;
        """
        if (schema, table) not in self._metadata_cache:
            curr = self.execute(query, params=(table, schema))
            self._metadata_cache[(schema, table)] = tuple(curr.fetchall())
        return self._metadata_cache[(schema, table)]

    def get_columns(self, table, schema='dbo'):
        return [ColumnMetadata(name, dt, nu == 'YES', con == 'PRIMARY KEY', table, df) for name, df, nu, dt, *_, con in self.md_cache(table, schema)]

    def get_primary_keys(self, table, schema='dbo'):
        return [name for name, *_, con in self.md_cache(table, schema) if con == 'PRIMARY KEY']

    def get_foreign_keys(self, table, schema='dbo'):
        return [ForeignKeyMetadata(name, dest_tab, dest_col, table) for name, *_, dest_tab, dest_col, con in self.md_cache(table, schema) if con == 'FOREIGN KEY']

    def sequence_exists(self, seq):
        raise NotImplementedError

    def extract_date(self, date_part, date_field):
        raise NotImplementedError

    def truncate_date(self, date_part, date_field):
        raise NotImplementedError

    def to_timestamp(self, date_field):
        raise NotImplementedError

    def from_timestamp(self, date_field):
        raise NotImplementedError

    def get_tables(self, schema="dbo"):
        result = self.execute(r"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=%s ORDER BY TABLE_NAME;", params=(schema,))
        return [row[0] for row in result.fetchall()]

    def execute(self, sql, params=(), **kwargs):
        self.log.debug(sql) # this is interpolated and incorrect
        curr = self.cursor(True)
        curr.execute(sql, params)
        return curr

class MssqlMetadata(playhouse.reflection.Metadata):

    column_map = {
        'int':IntegerField,
        'char':CharField,
        'varchar':CharField,
        'text':TextField,
        'date':DateField,
        'tinyint':IntegerField,
        'float':FloatField,
        'nvarchar':CharField,
        'varbinary':BlobField,
        'nchar':CharField,
        'datetime2':DateTimeField,
        'datetime':DateTimeField,
        'time':TimeField,
        'decimal':DecimalField,

    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.log = logger.getChild(self.__class__.__name__)
    
    def get_column_types(self, table, schema='dbo'):
        if not schema:
            schema = 'dbo'
        self.log.debug("%s.%s", schema, table)
        cols = self.database.get_columns(table, schema)
        return {col.name:self.column_map[col.data_type] for col in cols}, {}

    def get_columns(self, table, schema=None):
        schema = schema or 'dbo'
        return super().get_columns(table, schema)

    def get_foreign_keys(self, table, schema=None):
        schema = schema or 'dbo'
        return super().get_foreign_keys(table, schema)

    def get_primary_keys(self, table, schema=None):
        schema = schema or 'dbo'
        return super().get_primary_keys(table, schema)

    def get_indexes(self, table, schema=None):
        schema = schema or 'dbo'
        return super().get_indexes(table, schema)
        



_from_database = playhouse.reflection.Introspector.from_database

def from_database(cls, database, schema=None):
    try:
        return _from_database(database, schema)
    except ValueError as e:
        if isinstance(database, MssqlDatabase):
            if not schema:
                schema='dbo'
            return cls(MssqlMetadata(database), schema=schema)
        raise e

playhouse.reflection.Introspector.from_database = classmethod(from_database)