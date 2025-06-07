import pandas as pd
from supabase import create_client
from pymongo import MongoClient

# conexión a la base de datos operacional
url_operacional = 'https://ggvtnhsokxrroymxgres.supabase.co'
key_operacional = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImdndnRuaHNva3hycm95bXhncmVzIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc0ODYzNTgwNCwiZXhwIjoyMDY0MjExODA0fQ.iRMSBrcUSlA-IpiofR6xc4W6_Dq-smhhMs6sBKHk_dA'
supabase_op = create_client(url_operacional, key_operacional)

# conexión a la base de datos de MongoDB
uri = 'mongodb+srv://naza:chauflix123@chauflix.g5rhogq.mongodb.net/'
client = MongoClient(uri)
mongo = client['chauflix']

# conexión al datawarehouse
url_dw = 'https://hcwyzlprqjlwqwdrfrco.supabase.co'
key_dw = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imhjd3l6bHBycWpsd3F3ZHJmcmNvIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc0OTA3NjkzOSwiZXhwIjoyMDY0NjUyOTM5fQ.kslrFLfk4e6HRIPs60qjwna4XWiXIPiJSv7988QjLIo'
supabase_dw = create_client(url_dw, key_dw)

def extraer_tabla_mongo(nombre_tabla: str) -> pd.DataFrame:
    """
    Extrae una tabla de la base de datos MongoDB y la guarda en un DataFrame.
    """
    movies = mongo[nombre_tabla].find()
    df = pd.DataFrame(movies)
    df = df.drop(columns=['_id'])  # Eliminar la columna _id
    return df

def extraer_tabla_dw(nombre_tabla: str, cols:str='*') -> pd.DataFrame:
    """
    Extrae una tabla de la base de datos operacional y la guarda en un DataFrame.
    """
    response = supabase_dw.table(nombre_tabla).select(cols).execute()
    df = pd.DataFrame(response.data)
    return df

def cargar_tabla_datawarehouse(df: pd.DataFrame, nombre_tabla: str) -> None:
    """
    Carga el DataFrame transformado en la tabla del datawarehouse.
    """
    supabase_dw.schema("public").table(nombre_tabla).insert(df.to_dict(orient='records')).execute()

def extraer_ultima_fecha_rating_dw() -> str | None:
    """
    Extrae la última fecha de rating del datawarehouse y la devuelve como una cadena de texto con formato timestamp.
    """
    query = supabase_dw.table('fact_rating').select('date').order('date', desc=True).limit(1).execute()
    if query.data:
        date = query.data[0]['date']
        date = pd.to_datetime(date) + pd.Timedelta(days=1)
        date = date.strftime('%Y-%m-%d %H:%M:%S')
        return date
    else:
        return None

def extraer_nuevos_ratings_operacional(mongo):
    """
    Extrae los ratings desde un timestamp específica de la base de datos operacional y los devuelve como un DataFrame de pandas.
    """
    last_date = extraer_ultima_fecha_rating_dw()
    if last_date:
        query = mongo['ratings'].find({'timestamp': {'$gt': last_date}})
    
    df_ratings = pd.DataFrame(list(query))
    return df_ratings

def convertir_timestamp_a_fecha(ratings_df:pd.DataFrame) -> pd.DataFrame:
    """
    Convierte una serie de timestamps a fechas.
    """
    ratings_df['timestamp'] = pd.to_datetime(ratings_df['timestamp'])
    ratings_df['timestamp'] = ratings_df['timestamp'].dt.date

    ratings_df.rename(columns={'timestamp': 'date'}, inplace=True)

    return ratings_df

def transformar_ratings(ratings_df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma el DataFrame de ratings para que tenga la estructura adecuada.
    """
    ratings_df['rating'] = ratings_df['rating'] * 2  # Convertir a escala de 0 a 10
    
    ratings_df = convertir_timestamp_a_fecha(ratings_df)

    return ratings_df

def cargar_ratings_dw(ratings_df: pd.DataFrame):
    """
    Carga el DataFrame de ratings al datawarehouse.
    """
    ratings_df = transformar_ratings(ratings_df)
    
    cargar_tabla_datawarehouse(ratings_df, 'fact_rating')

def extraer_peliculas_dw():
    """
    Extrae las películas del datawarehouse y las devuelve como un DataFrame de pandas.
    """
    query = supabase_dw.table('dim_movie').select('*').execute()
    df_movies = pd.DataFrame(query.data)
    return df_movies

if __name__ == "__main__":
    # extraer ultimos ratings desde MongoDB
    df_ratings = extraer_nuevos_ratings_operacional(mongo)
    if not df_ratings.empty:
        # cargar ratings al datawarehouse
        cargar_ratings_dw(df_ratings)
        # extraer todos los ratings del datawarehouse
        df_ratings_dw = extraer_tabla_dw('fact_rating')
        # calcular el promedio de ratings por película y agregarlo a la tabla de películas
        df_movies = extraer_peliculas_dw()
        merged = df_movies.merge(df_ratings_dw.groupby('movie_id')['rating'].mean().reset_index(), on='movie_id', how='left')
        df_movies['mean_rating'] = merged['rating']
        # cargar la tabla de películas actualizada al datawarehouse con upsert
        supabase_dw.schema("public").table('dim_movies').upsert(df_movies.to_dict(orient='records')).execute()
    else:
        print("No se encontraron nuevos ratings para cargar al datawarehouse.")