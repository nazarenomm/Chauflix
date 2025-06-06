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

def extraer_tabla_supabase(nombre_tabla: str) -> pd.DataFrame:
    """
    Extrae una tabla de la base de datos operacional y la guarda en un DataFrame.
    """
    response = supabase_op.table(nombre_tabla).select('*').execute()
    df = pd.DataFrame(response.data)
    return df

#%% dim_movie

movies = extraer_tabla_mongo('movies')

def transformar_peliculas(movies_df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma el DataFrame de películas para que tenga la estructura adecuada.
    """
    movies_df.drop(columns=['release_year', 'duration_min'], inplace=True)
    # extraer licencias
    licenses_df = extraer_tabla_supabase('licenses')
    # extraer pagos de licencias
    licenses_payment_df = extraer_tabla_supabase('license_payments')
    # unir licencias y pagos de licencias
    merge = licenses_df.merge(licenses_payment_df, left_on='id', right_on='license_id', how='left')
    merge = merge[['movie_id', 'duration', 'date']]

    # agregar expiration date a las películas
    for row in merge.itertuples():
        movie_id = row.movie_id
        duration = row.duration
        date = row.date

        date = pd.to_datetime(date).date()
        expiration_date = date + pd.Timedelta(days=duration)
        expiration_date = expiration_date.strftime('%Y-%m-%d')

        movies_df.loc[movies_df['id'] == movie_id, 'license_expiration'] = expiration_date
    
    # extraer ratings
    ratings_df = extraer_tabla_mongo('ratings')
    ratings_df['rating'] = ratings_df['rating']*2

    # calcular la media de ratings por película y agregarla al DataFrame de películas
    for row in movies_df.itertuples():
        movie_id = row.id
        mean_rating = ratings_df[ratings_df['movie_id'] == movie_id]['rating'].mean()
        movies_df.loc[movies_df['id'] == movie_id, 'rating'] = mean_rating

    # extraer género principal
    for row in movies_df.itertuples():
        genres = row.genres
        if genres:
            primer_genero = genres[0]
        else:
            primer_genero = None
        movies_df.loc[movies_df['id'] == row.id, 'genres'] = primer_genero
    
    movies_df.rename(columns={'genres': 'genre', 'rating': 'mean_rating'}, inplace=True)
    movies_df.dropna(inplace=True) # polemico, no me dejaba subir un na

    return movies_df

def cargar_tabla_datawarehouse(df: pd.DataFrame, nombre_tabla: str) -> None:
    """
    Carga el DataFrame transformado en la tabla del datawarehouse.
    """
    supabase_dw.schema("public").table(nombre_tabla).insert(df.to_dict(orient='records')).execute()

movies = transformar_peliculas(movies)
cargar_tabla_datawarehouse(movies, 'dim_movie')

#%% dim_user

users_df = extraer_tabla_supabase('users')

def transformar_usuarios(users_df: pd.DataFrame) -> pd.DataFrame:
    users_df = users_df[['id', 'birth_date', 'country_id', 'gender']]
    countries_df = extraer_tabla_supabase('country')
    countries_df = countries_df[['id', 'name']]
    merged_df = pd.merge(users_df, countries_df, left_on='country_id', right_on='id', how='left')
    merged_df.drop(columns=['country_id', 'id_y'], inplace=True)
    merged_df.rename(columns={'id_x': 'id', 'name': 'country'}, inplace=True)
    return merged_df

users_df = transformar_usuarios(users_df)
cargar_tabla_datawarehouse(users_df, 'dim_user')