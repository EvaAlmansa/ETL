import sqlalchemy
from sqlalchemy.engine import create_engine
from sqlalchemy import Column, ForeignKey, Float, String, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.orm import sessionmaker


Base = declarative_base()


class IndustryCode(Base):
    __tablename__ = 'industrycode'
    id = Column(Integer, primary_key=True, autoincrement=True)
    industry_code = Column(String, unique=True)


class Industry(Base):
    __tablename__ = 'industry'
    id = Column(Integer, primary_key=True, autoincrement=True)
    count = Column(Integer, unique=False)
    sum_booking = Column(Float, unique=False)

    industry_code = Column(Integer, ForeignKey('industrycode.industry_code'))
    industry = relationship(IndustryCode)


class Merchant(Base):
    __tablename__ = 'merchant'
    id = Column(Integer, primary_key=True, autoincrement=True)
    merchant_uuid = Column(String, unique=True)
    count = Column(Integer, unique=False)
    sum_booking = Column(Float, unique=False)

    industry_code = Column(Integer, ForeignKey('industrycode.industry_code'))
    industry = relationship(IndustryCode)


class MonthlyDistribution(Base):
    __tablename__ = 'monthlydistribution'
    id = Column(Integer, primary_key=True, autoincrement=True)
    month = Column(Integer, unique=False)
    sum_booking = Column(Float, unique=False)

    industry_code = Column(Integer, ForeignKey('industrycode.industry_code'))
    industry = relationship(IndustryCode)

class CreateDatabase:
    def __init__(self, output_path, name_db):
        database_location = "sqlite:///" + output_path + name_db + ".sqlite"
        self.new_engine = sqlalchemy.create_engine(database_location)
        # Create / Build Schema
        Base.metadata.create_all(self.new_engine)
        Session = sessionmaker(bind=self.new_engine)
        self.session = Session()
