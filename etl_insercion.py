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

# extracción de views de MongoDB despues de un timestamp
def extraer_reproducciones_desde_timestamp(db_mongo, timestamp):
    reproducciones = db_mongo['views'].find(
        {'watched_at': {'$gt': timestamp}}
    )
    return pd.DataFrame(reproducciones)

def transformar_reproducciones(df):
    """
    Transforma el DataFrame de vistas para que tenga la estructura adecuada.
    """

    df['watched_at'] = pd.to_datetime(df['watched_at'])
    df = df[['user_id', 'watched_at', 'minutes_watched', 'movie_id']]
    df['watched_at'] = df['watched_at'].apply(lambda x: x.isoformat() if pd.notnull(x) else None)

    return df

def cargar_reproducciones(df, supabase_client):
    """
    Carga el DataFrame de reproducciones al datawarehouse.
    """
    def pop_batch(df, batch_size): # helper para cargar en tandas
        batch = df.iloc[:batch_size].copy()
        df.drop(index=df.index[:batch_size], inplace=True)
        return batch
    
    while not df.empty:
        batch = pop_batch(df, 100_000)
        response = supabase_client.table('fact_views').insert(batch.to_dict(orient='records')).execute()
        if response.error:
            print(f"Error al cargar el batch: {response.error}")
        else:
            print(f"Batch cargado exitosamente: {len(batch)} registros")

if __name__ == "__main__":
    try:
        # obtenemos el último timestamp de la tabla fact_views
        ultimo_registro = supabase_dw.table('fact_views').select('watched_at').order('watched_at', desc=True).limit(1).execute()
        if ultimo_registro.data:
            ultimo_timestamp = ultimo_registro.data[0]['watched_at']
        else:
            # Si no hay registros, tomar un timestamp muy antiguo
            ultimo_timestamp = "1980-01-01T00:00:00"

        # extraemos las nuevas reproducciones desde MongoDB
        nuevas_reproducciones = extraer_reproducciones_desde_timestamp(mongo, ultimo_timestamp)

        # transformamos y cargamos las nuevas reproducciones al datawarehouse
        if not nuevas_reproducciones.empty:
            nuevas_reproducciones = transformar_reproducciones(nuevas_reproducciones)
            cargar_reproducciones(nuevas_reproducciones, supabase_dw)
        else:
            print("No se encontraron nuevas reproducciones desde el último timestamp.")
            
    except Exception as e:
        print(f"Error al procesar las reproducciones: {e}")
        raise