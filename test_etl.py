import pretty_errors
from etl import CustomerETL, DateETL, FilmETL, StoreETL, RentalETL



etl = DateETL()
raw_data = etl.extract_data()
transformed_data = etl.transform_data(raw_data)
etl.load_data(transformed_data, table_name='dim_date')