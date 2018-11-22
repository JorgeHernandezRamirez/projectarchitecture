from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.sql.sqltypes import String, DateTime, NullType, Date, Time

class StringLiteral(String):
    def literal_processor(self, dialect):
        super_processor = super().literal_processor(dialect)

        def process(value):
            if isinstance(value, int):
                return str(value)
            if not isinstance(value, str):
                value = str(value)
            result = super_processor(value)
            if isinstance(result, bytes):
                result = result.decode(dialect.encoding)
            return result

        return process

class CustomDialect(DefaultDialect):
    colspecs = {
        String: StringLiteral,
        DateTime: StringLiteral,
        Date: StringLiteral,
        Time: StringLiteral,
        NullType: StringLiteral
    }

