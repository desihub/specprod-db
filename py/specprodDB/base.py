# Licensed under a 3-clause BSD style license - see LICENSE.rst
# -*- coding: utf-8 -*-
"""
specprodDB.base
===============

Base classes for all ORM definitions.
"""
from sqlalchemy.orm import (DeclarativeBase, declarative_mixin, declared_attr)


class Base(DeclarativeBase):
    """SQLAlchemy 2.0 replacement for ``Base = declarative_base()``.
    """
    pass


def schema_mixin_factory(schemaname):
    """Create a mixin class with the schema name set to `schemaname`.

    Parameters
    ----------
    schemaname : :class:`str`
        Name of a schema, typically a specprod name.

    Returns
    -------
    :class:`object`
        A mixin object with the schema name set.
    """
    @declarative_mixin
    class SchemaMixin(object):
        """Mixin class to allow schema name to be changed at runtime. Also
        automatically sets the table name.
        """

        @declared_attr.directive
        def __tablename__(cls):
            return cls.__name__.lower()

        @declared_attr.directive
        def __table_args__(cls):
            return {'schema': schemaname}

    return SchemaMixin
