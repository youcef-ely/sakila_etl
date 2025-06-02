import os
import pretty_errors
from dotenv import load_dotenv
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy import create_engine, Column, Integer, String, Float, Date, Text, ForeignKey

dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(dotenv_path)

MYSQL_WAREHOUSE_HOST = os.environ.get('MYSQL_WAREHOUSE_HOST')  
MYSQL_WAREHOUSE_DATABASE = os.environ.get('MYSQL_WAREHOUSE_DATABASE')
MYSQL_WAREHOUSE_USER = os.environ.get('MYSQL_WAREHOUSE_USER')
MYSQL_WAREHOUSE_PORT= os.environ.get('MYSQL_WAREHOUSE_PORT')
MYSQL_WAREHOUSE_PASSWORD = os.environ.get('MYSQL_WAREHOUSE_PASSWORD')



DATABASE_URL = 'sqlite:///sakila_dw.db'
Base = declarative_base()

class DimDate(Base):
    __tablename__ = 'dim_date'
    date_key = Column(Integer, primary_key=True)
    full_date = Column(Date, nullable=False)
    rental_month = Column(String(20), nullable=False)
    rental_week = Column(Integer, nullable=False)

class DimStore(Base):
    __tablename__ = 'dim_store'
    store_key = Column(Integer, primary_key=True)
    address = Column(String(50), nullable=False)
    city = Column(String(50), nullable=False)
    state = Column(String(50), nullable=False)
    country = Column(String(50), nullable=False)

class DimFilm(Base):
    __tablename__ = 'dim_film'
    film_key = Column(Integer, primary_key=True)
    title = Column(String(200), nullable=False)
    description = Column(Text, nullable=False)
    length = Column(Integer)
    category = Column(String(50))  # lowercase for naming consistency

class DimClient(Base):
    __tablename__ = 'dim_client'
    customer_key = Column(Integer, primary_key=True)
    store_id = Column(Integer, ForeignKey('dim_store.store_key'))
    full_name = Column(String(100))
    email = Column(String(100), nullable=False)
    city = Column(String(50))
    state = Column(String(50))

    store = relationship("DimStore", backref="clients")

class FactRental(Base):
    __tablename__ = 'fact_rental'
    rental_key = Column(Integer, primary_key=True, autoincrement=True)
    customer_key = Column(Integer, ForeignKey('dim_client.customer_key'))
    film_key = Column(Integer, ForeignKey('dim_film.film_key'))
    store_key = Column(Integer, ForeignKey('dim_store.store_key'))
    date_key = Column(Integer, ForeignKey('dim_date.date_key'))
    amount = Column(Float, nullable=False)

    customer = relationship("DimClient")
    film = relationship("DimFilm")
    store = relationship("DimStore")
    date = relationship("DimDate")


def create_datawarehouse():
    engine = create_engine(
                DATABASE_URL,
                #f'mysql+mysqlconnector://{MYSQL_WAREHOUSE_USER}:{MYSQL_WAREHOUSE_PASSWORD}@{MYSQL_WAREHOUSE_HOST}:{MYSQL_WAREHOUSE_PORT}/{MYSQL_WAREHOUSE_DATABASE}'
                echo=True
            )

    Base.metadata.create_all(engine)

if __name__ == "__main__":
    create_datawarehouse()