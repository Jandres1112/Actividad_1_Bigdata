from sqlalchemy import create_engine, text
import pandas as pd



class Modelo:
    
    def __init__(self, host="",port="",nombredb="",user="",password=""):
        self.host = host
        self.port = port
        self.nombredb = nombredb
        self.user = user
        self.password = password
        self.conexion = None
        self.connexion()
        self.df = pd.DataFrame()


    def connexion(self):
        try:            
            self.conexion = create_engine(f'postgresql+psycopg://{self.user}:{self.password}@{self.host}:{self.port}/{self.nombredb}')
            with self.conexion.connect() as conexion:
                print("se conecto de manera exitosa a la base de datos")
        except print(0):
                print("error conexion a la base de datos")
            
              
            
    def create_schema(self, nombre_schema):
        try:
            with self.conexion.connect() as conexion:
                create_schema = f'CREATE SCHEMA IF NOT EXISTS {nombre_schema};'
                conexion = conexion.execution_options(isolation_level="AUTOCOMMIT")
                conexion.execute(text(create_schema))
                print("creacion Schema exitoso")
        except print(0):
            print("error creacion schema") 

    def create_table(self,ruta_sql=""):
        try:
            with open(ruta_sql,'r') as file:
                script_tabla = file.read()
            with self.conexion.connect() as conexion:
                conexion = conexion.execution_options(isolation_level="AUTOCOMMIT")
                conexion.execute(text(script_tabla))
                print("creacion exitosa de tabla")
            print(0)
        except print(0):
            print("creacion erronea de tabla")  

            
    
    def insert_df(self,ruta_xlsx="", nombre_schema="", nombre_tabla="", tipo_insert='append', tipo="csv"):
        try:
            Data = pd.read_csv(ruta_xlsx,index_col=False, encoding='latin1')
            Data.columns = Data.columns.str.lower()
            df = pd.DataFrame(Data)
            df['price'] = df['price'].str.replace('.', '', regex=False)
            df['price'] = pd.to_numeric(df['price'], errors='coerce')
            print("Se creo el dataframe")

        except print(0):
            print("No se creo el dataframe")
            return
        
        try:
            df.to_sql(name=nombre_tabla, con=self.conexion, schema=nombre_schema, if_exists=tipo_insert, index=False)
            self.df = df
            print("Se inserto correctamente")
        except print(0):
            print("No se inserto")
    
    def test_integridad(self):
         
            with self.conexion.connect() as conexion:
                consulta_data = f'SELECT COUNT(*) AS cantidad_datos FROM house_price.propiedad;'
                conexion = conexion.execution_options(isolation_level="AUTOCOMMIT")
                result = conexion.execute(text(consulta_data))
                result2 = pd.DataFrame(result)
                print("Prueba de integridad insumo total: {} vs db total: {}".format(len(self.df),result2))
        


