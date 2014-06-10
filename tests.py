import json
import uuid

import pilo
try:
    import unittest2 as unittest
except ImportError:
    import unittest

from sqlalchemy import MetaData, Table, Unicode, Text, TypeDecorator, Column, create_engine
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session

import sqlalchemy_pilo as sap


meta_data = MetaData()

Session = scoped_session(sessionmaker())

engine = None

Base = declarative_base()


def generate_id():
    return uuid.uuid4().hex


class Form(pilo.Form):

    a = pilo.fields.Integer()

    b = pilo.fields.Float(default=121.12)

    c = pilo.fields.Tuple((pilo.fields.String(), pilo.fields.Boolean()))

    d = pilo.fields.Dict(pilo.fields.String(), pilo.fields.String())


class AForm(pilo.Form):

    _type_ = pilo.fields.Type().abstract()

    a = pilo.fields.Integer()

    b = pilo.fields.Float(default=1.13)


class BForm(AForm):

    _type_ = pilo.fields.Type().instance('a.b.v1')

    c = pilo.fields.Tuple((pilo.fields.String(), pilo.fields.Boolean()))


class CForm(AForm):

    _type_ = pilo.fields.Type().instance('a.c.v1')

    d = pilo.fields.Dict(pilo.fields.String(), pilo.fields.String())


json_tests = Table(
    'json_tests',
    meta_data,
    Column('id', Text, primary_key=True, default=generate_id),
    Column('data', postgresql.JSON, nullable=False),
)

class JSONTest(Base):

    __table__ = json_tests

sap.as_form(JSONTest.data, Form)


class PolymorphicJSONTest(Base):

    __table__ = json_tests

sap.as_form(PolymorphicJSONTest.data, AForm._type_)


class MutableJSONTest(Base):

    __table__ = json_tests

sap.as_form(MutableJSONTest.data, Form, mutable=True)


class MutablePolymorphicJSONTest(Base):

    __table__ = json_tests

sap.as_form(MutablePolymorphicJSONTest.data, AForm._type_, mutable=True)


class EncodedJSON(TypeDecorator):

    # TypeDecorator

    impl = Text

    def process_bind_param(self, value, dialect):
        if value is not None:
            value = json.dumps(value)
        return value

    def process_result_value(self, value, dialect):
        if value is not None:
            value = json.loads(value)
        return value


encoded_json_tests = Table(
    'encoded_json_tests',
    meta_data,
    Column('id', Text, primary_key=True, default=generate_id),
    Column('data', EncodedJSON, nullable=False),
)

class EncodedJSONTest(Base):

    __table__ = encoded_json_tests


sap.as_form(EncodedJSONTest.data, Form)


class TestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        global engine

        super(TestCase, cls).setUpClass()
        engine = create_engine(
            'postgresql://sqlalchemy_pilo:sqlalchemy_pilo@localhost/sqlalchemy_pilo',
            echo=False,
        )
        meta_data.create_all(engine)
        Session.configure(bind=engine)


class TestWithJSONColumn(TestCase):

    def test_form(self):
        v = JSONTest(
            data=Form(
                a=123,
                b=12.12,
                c=('a', 1),
                d={'a': 'tag'},
            )
        )
        Session.add(v)
        Session.commit()
        self.assertEqual(v.data.a, 123)

    def test_form_write_invalid(self):
        v = JSONTest()
        with self.assertRaises(ValueError) as ex:
            v.data = {'a': []}
        self.assertIn('a - "[]" is not an integer', str(ex.exception))

    def test_form_read_invalid(self):
        id = generate_id()
        Session.execute(json_tests.insert().values(id=id, data={'a': []}))
        with self.assertRaises(ValueError) as ex:
            v = Session.query(JSONTest).get(id)
        self.assertIn('a - "[]" is not an integer', str(ex.exception))

    def test_polymorphic_form(self):
        v = PolymorphicJSONTest(
            data=CForm(
                a=123,
                b=12.12,
                d={'a': 'tag'},
            ))
        Session.add(v)
        self.assertEqual(v.data._type_, 'a.c.v1')
        Session.commit()
        v = Session.query(PolymorphicJSONTest).filter_by(id=v.id).one()
        self.assertEqual(v.data._type_, 'a.c.v1')
        self.assertIsInstance(v.data, CForm)

    def test_mutable_form(self):
        v = MutableJSONTest(
            data=Form(
                a=123,
                b=12.12,
                c=('a', 1),
                d={'a': 'tag'},
            )
        )
        Session.add(v)
        self.assertIn(v, Session.new)
        Session.commit()
        self.assertNotIn(v, Session.dirty)
        v.data.a = 321
        self.assertIn(v, Session.dirty)
        Session.commit()
        self.assertNotIn(v, Session.dirty)

    def test_mutable_polymorphic_form(self):
        v = MutablePolymorphicJSONTest(
            data=CForm(
                a=123,
                b=12.12,
                d={'a': 'tag'},
            ))
        Session.add(v)
        self.assertIn(v, Session.new)
        Session.commit()
        self.assertIsInstance(v.data, CForm)
        self.assertNotIn(v, Session.dirty)
        v.data.a = 321
        self.assertIn(v, Session.dirty)
        Session.commit()
        self.assertIsInstance(v.data, CForm)
        self.assertNotIn(v, Session.dirty)
