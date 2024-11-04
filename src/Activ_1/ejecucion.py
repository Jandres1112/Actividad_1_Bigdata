import pkg_resources
from modelo import Modelo


ruta = pkg_resources.resource_filename(__name__,'static')
host="localhost"
port="5433"
nombredb="postgres"
user="postgres"
password="1234"
nombre_schema= "house_price"
nombre_tabla="propiedad"
ruta_sql = "src\\Activ_1\\Estaticos\\sql\\script_creacion.sql".format(ruta)
ruta_csv = "src\\Activ_1\\Estaticos\\xlsx\\House_price.xlsx".format(ruta)
modelo = Modelo(host,port,nombredb,user,password)
modelo.create_schema(nombre_schema)
modelo.create_table(ruta_sql)
modelo.insert_df(ruta_xlsx=ruta_csv,nombre_schema=nombre_schema, nombre_tabla=nombre_tabla, tipo_insert='append')
modelo.test_integridad()