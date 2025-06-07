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

def extraer_tabla_mongo(mongo, nombre_tabla: str) -> pd.DataFrame:
    """
    Extrae una tabla de la base de datos MongoDB y la guarda en un DataFrame.
    """
    movies = mongo[nombre_tabla].find()
    df = pd.DataFrame(movies)
    df = df.drop(columns=['_id'])  # Eliminar la columna _id
    return df

def extraer_tabla_supabase(supabase_op, nombre_tabla: str, cols:str='*') -> pd.DataFrame:
    """
    Extrae una tabla de la base de datos operacional y la guarda en un DataFrame.
    """
    response = supabase_op.table(nombre_tabla).select(cols).execute()
    df = pd.DataFrame(response.data)
    return df

def extraer_tabla_datawarehouse(supabase_dw, nombre_tabla: str, cols:str='*') -> pd.DataFrame:
    """
    Extrae una tabla del Datawarehouse y la guarda en un DataFrame.
    """
    response = supabase_dw.table(nombre_tabla).select(cols).execute()
    df = pd.DataFrame(response.data)
    return df

def cargar_tabla_datawarehouse(df: pd.DataFrame, nombre_tabla: str) -> None:
    """
    Carga el DataFrame transformado en la tabla del datawarehouse.
    """
    supabase_dw.schema("public").table(nombre_tabla).insert(df.to_dict(orient='records')).execute()

#%% ----------dim_movieS

def validar_si_esta_en_datawarehouse(movies_mongo: pd.DataFrame, movies_dw: pd.DataFrame) -> pd.DataFrame:
    """
    Valida si las películas de MongoDB ya están en el datawarehouse.
    Retorna un DataFrame con las películas que no están en el datawarehouse.
    """
    df = movies_mongo[~movies_mongo['id'].isin(movies_dw['id'])]
    
    return df

def agregar_expiration_date(movies_df: pd.DataFrame, licenses_df: pd.DataFrame, licenses_payment_df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega la fecha de expiración a las películas en el DataFrame.
    """

    # unir licencias y pagos de licencias
    merge = licenses_df.merge(licenses_payment_df, left_on='id', right_on='license_id', how='left')
    merge = merge[['movie_id', 'duration', 'date']]

    # calcular y agregar fecha de expiración
    for row in merge.itertuples():
        movie_id = row.movie_id
        duration = row.duration
        date = row.date

        date = pd.to_datetime(date).date()
        expiration_date = date + pd.Timedelta(days=duration)
        expiration_date = expiration_date.strftime('%Y-%m-%d')

        movies_df.loc[movies_df['id'] == movie_id, 'license_expiration'] = expiration_date

    return movies_df

def calcular_media_rating(ratings_df: pd.DataFrame, movies_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula y agregar la media de ratings
    """
    ratings_df['rating'] = ratings_df['rating']*2  # Convertir a escala de 0 a 10

    for row in movies_df.itertuples():
        movie_id = row.id
        mean_rating = ratings_df[ratings_df['movie_id'] == movie_id]['rating'].mean()
        movies_df.loc[movies_df['id'] == movie_id, 'rating'] = mean_rating
    
    return movies_df

def extraer_genero_principal(movies_df: pd.DataFrame) -> pd.DataFrame:
    """
    Extrae el género principal de las películas.
    """
    for row in movies_df.itertuples():
        genres = row.genres
        if genres:
            primer_genero = genres[0]
        else:
            primer_genero = None
        movies_df.loc[movies_df['id'] == row.id, 'genres'] = primer_genero

    return movies_df
    
def transformar_peliculas(movies_df: pd.DataFrame, ratings_df: pd.DataFrame, licenses_df: pd.DataFrame, licenses_payment_df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma el DataFrame de películas para que tenga la estructura adecuada.
    """
    movies_df.drop(columns=['release_year', 'duration_min'], inplace=True)
    
    movies_df = agregar_expiration_date(movies_df, licenses_df, licenses_payment_df)
    
    movies_df = calcular_media_rating(ratings_df, movies_df) # calcular la media de ratings por película y agregarla al DataFrame de películas

    movies_df = extraer_genero_principal(movies_df) # extraer género principal y reemplazar la lista
    
    movies_df.rename(columns={'genres': 'genre', 'rating': 'mean_rating'}, inplace=True)

    return movies_df

# se extraen las tablas necesarias
licenses_df = extraer_tabla_supabase('licenses')
licenses_payment_df = extraer_tabla_supabase('license_payments')
ratings = extraer_tabla_mongo('ratings')
movies = extraer_tabla_mongo('movies')

# se transforma la tabla de películas
movies = transformar_peliculas(movies, ratings, licenses_df, licenses_payment_df)

# se cargan las películas transformadas en el datawarehouse
cargar_tabla_datawarehouse(movies, 'dim_movie')

#%% ------------dim_user
def agregar_nombre_pais(users_df: pd.DataFrame, countries_df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega el nombre del país a los usuarios.
    """
    # join de usuarios y paises
    merged_df = pd.merge(users_df, countries_df, left_on='country_id', right_on='id', how='left')
    merged_df.drop(columns=['country_id', 'id_y'], inplace=True)

    merged_df.rename(columns={'id_x': 'id', 'name': 'country'}, inplace=True)

    return merged_df
    
    return users_df
def transformar_usuarios(users_df: pd.DataFrame, countries_df: pd.DataFrame) -> pd.DataFrame:
    users_df = users_df[['id', 'birth_date', 'country_id']]

    users_df = agregar_nombre_pais(users_df, countries_df)

    return users_df

users_df = extraer_tabla_supabase('users')
countries_df = extraer_tabla_supabase('country')

users_df = transformar_usuarios(users_df, countries_df)
cargar_tabla_datawarehouse(users_df, 'dim_user')

#%% -------------fact_cancelation

def obtener_plan_cancelado(subscriptions_df: pd.DataFrame, plans_df:pd.DataFrame) -> pd.DataFrame:
    """
    Obtiene el plan cancelado por cada usuario en la última fecha de suscripción.
    """
    # join de suscripciones y planes para obtener el nombre del plan
    subscriptions_df.merge(plans_df, left_on='plan_id', right_on='id')

    # ordenar por fecha y eliminar duplicados para obtener el último plan de cada usuario
    subscriptions_df = subscriptions_df.sort_values(by=['user_id', 'date'], ascending=False)
    subscriptions_df = subscriptions_df.drop_duplicates(subset=['user_id'], keep='first')

    return subscriptions_df

def transformar_cancelaciones(subscriptions_df: pd.DataFrame, plans_df: pd.DataFrame, cancelations_df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma el DataFrame de cancelaciones para que tenga la estructura adecuada.
    """
    subscriptions_df = obtener_plan_cancelado(subscriptions_df, plans_df)

    # agregamos el plan cancelado a las cancelaciones
    cancelations_df = cancelations_df.merge(subscriptions_df, on='user_id', how='left', suffixes=('', '_sub'))
    cancelations_df.drop(columns=['plan_id', 'date_sub', 'id_sub'], inplace=True)

    return cancelations_df

cancelations_df = extraer_tabla_supabase('cancelations')
plans_df = extraer_tabla_supabase('plans', cols='id, plan_name')
subscriptions_df = extraer_tabla_supabase('subscriptions')

cancelations_df = transformar_cancelaciones(subscriptions_df, plans_df, cancelations_df)
cargar_tabla_datawarehouse(cancelations_df, 'fact_cancelation')

#%% -----------fact_subscriptions_payment
def agregar_bool_renovation(subscriptions_df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega una columna booleana que indica si la suscripción es una renovación.
    """
    # ordenar por fecha y eliminar duplicados para obtener la primera suscripción de cada usuario
    subscriptions_df["renovation"] = subscriptions_df.sort_values("date").duplicated(subset=["user_id"], keep="first")
    subscriptions_df.rename(columns={'id_x': 'id'}, inplace=True)

    return subscriptions_df

def transformar_suscripciones(subscriptions_df: pd.DataFrame, plans_df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma el DataFrame de suscripciones para que tenga la estructura adecuada.
    """
    # join de suscripciones y planes
    subscriptions_df = subscriptions_df.merge(plans_df, left_on="plan_id", right_on="id", how="left")

    # agregamos la columna booleana de renovación
    subscriptions_df = agregar_bool_renovation(subscriptions_df)

    fact_subscription_payment_df = subscriptions_df[["id", "user_id", "date", "plan_name", "renovation", "price"]] # nos quedamos con las columnas necesarias
    fact_subscription_payment_df.rename(columns={'plan_name': 'plan', 'price': 'pricing'}, inplace=True)

    return fact_subscription_payment_df

fact_subscription_df = transformar_suscripciones(subscriptions_df, plans_df)
cargar_tabla_datawarehouse(fact_subscription_df, 'fact_subscriptions_payment')

#%% ---------------------fact_license_payments

def unir_licencias_y_pagos(licenses: list[dict], license_payments: list[dict]) -> list[dict]:
    """
    Une las licencias con los pagos de licencias para obtener la información necesaria.
    """
    fact_license_payment_data = []
    for lp in license_payments:
        license_info = next((l for l in licenses if l["id"] == lp["license_id"]), None)
        if license_info:
            fact_license_payment_data.append({
                "id": lp["id"],
                "price": license_info["price"],
                "movie_id": license_info["movie_id"],
                "date": str(pd.to_datetime(lp["date"]).date()),  # Convertir a string
                "date_next_payment": str((pd.to_datetime(lp["date"]) + pd.Timedelta(days=license_info["duration"])).date())  # Convertir a string
            })
    return fact_license_payment_data

def filtrar_por_peliculas_en_dw(fact_license_payment_data: list[dict]) -> list[dict]:
    """
    Filtra los pagos de licencias para que solo incluya películas que están en la tabla dim_movie del datawarehouse.
    """
    dim_movies = supabase_dw.table("dim_movie").select("*").execute().data
    dim_movie_ids = [dm["id"] for dm in dim_movies]
    fact_license_payment_data = [
        row for row in fact_license_payment_data if row["movie_id"] in dim_movie_ids
    ]
    return fact_license_payment_data

def transformar_pagos_licencias(licenses: pd.DataFrame, license_payments: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma el DataFrame de pagos de licencias para que tenga la estructura adecuada.
    """
    fact_license_payment_data = unir_licencias_y_pagos(licenses.to_dict(orient='records'), license_payments.to_dict(orient='records'))
    
    fact_license_payment_data = filtrar_por_peliculas_en_dw(fact_license_payment_data)

    return pd.DataFrame(fact_license_payment_data)

licenses = extraer_tabla_supabase('licenses')
license_payments = extraer_tabla_supabase('license_payments')

fact_license_payments_df = transformar_pagos_licencias(licenses, license_payments)
cargar_tabla_datawarehouse(fact_license_payments_df, 'fact_license_payments')

#%% ---------------------fact_ratings
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

ratings_df = extraer_tabla_mongo('ratings')
ratings_df = transformar_ratings(ratings_df)
cargar_tabla_datawarehouse(ratings_df, 'fact_ratings')

#%% fact_views
def transformar_vistas(views_df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma el DataFrame de vistas para que tenga la estructura adecuada.
    """
    views_df['watched_at'] = pd.to_datetime(views_df['watched_at'])
    views_df = views_df[['user_id', 'watched_at', 'minutes_watched', 'movie_id']]
    views_df['watched_at'] = views_df['watched_at'].apply(lambda x: x.isoformat() if pd.notnull(x) else None)

    return views_df

def cargar_reproducciones_datawarehouse(views_df: pd.DataFrame) -> None:
    """
    Carga el DataFrame de reproducciones en la tabla del datawarehouse.
    """
    def pop_batch(df, batch_size): # helper para cargar en tandas
        batch = df.iloc[:batch_size].copy()
        df.drop(index=df.index[:batch_size], inplace=True)
        return batch
    
    while not views_df.empty:
        batch = pop_batch(views_df, 100_000)
        cargar_tabla_datawarehouse(batch, 'fact_views')

views_df = extraer_tabla_mongo('views')
views_df = transformar_vistas(views_df)
cargar_reproducciones_datawarehouse(views_df)