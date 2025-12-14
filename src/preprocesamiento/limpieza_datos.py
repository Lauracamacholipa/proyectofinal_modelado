# src/preprocesamiento/limpieza_datos.py
import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

def limpiar_datos_fifa():
    """Ejecuta el pipeline completo de limpieza de datos FIFA"""
    
    print("Limpieza de datos FIFA")
    print("=" * 40)
    
    # Configuración de rutas
    RUTA_ENTRADA = "data/raw/database.sqlite"
    RUTA_SALIDA = "data/processed/dia1/datos_limpios.sqlite"
    
    # 1. Carga de datos
    print("\n1. Cargando datos...")
    try:
        conn = sqlite3.connect(RUTA_ENTRADA)
        datos = pd.read_sql_query("SELECT * FROM Player_Attributes", conn)
        players_info = pd.read_sql_query(
            "SELECT player_api_id, player_name, birthday FROM Player", conn
        )
        conn.close()
        
        print(f"   Datos cargados: {datos.shape[0]} filas, {datos.shape[1]} columnas")
    except Exception as e:
        print(f"   Error al cargar datos: {e}")
        return
    
    # 2. Fusión y creación de posición inferida
    print("\n2. Preparando datos...")
    datos = pd.merge(datos, players_info, on='player_api_id', how='left')
    
    # Inferir posición basada en atributos
    def inferir_posicion(row):
        """Inferir la posición del jugador basándose en sus atributos"""
        try:
            # Detectar porteros
            gk_attrs = ['gk_diving', 'gk_handling', 'gk_kicking', 'gk_positioning', 'gk_reflexes']
            if any(pd.notnull(row.get(attr, 0)) and row.get(attr, 0) > 50 for attr in gk_attrs if attr in row):
                return 'Portero'
            
            # Calcular puntajes por área
            atributos_defensa = ['marking', 'standing_tackle', 'sliding_tackle', 'interceptions']
            atributos_medio = ['vision', 'short_passing', 'long_passing', 'ball_control']
            atributos_ataque = ['finishing', 'shot_power', 'long_shots', 'positioning']
            
            def promedio_atributos(attrs):
                valores = [row.get(attr, 0) for attr in attrs if attr in row and pd.notnull(row.get(attr, 0))]
                return np.mean(valores) if valores else 0
            
            score_def = promedio_atributos(atributos_defensa)
            score_med = promedio_atributos(atributos_medio)
            score_ataq = promedio_atributos(atributos_ataque)
            
            if score_ataq > max(score_def, score_med):
                return 'Delantero'
            elif score_med > max(score_def, score_ataq):
                return 'Mediocampista'
            elif score_def > max(score_med, score_ataq):
                return 'Defensa'
            else:
                return 'Versatil'
        except:
            return 'Desconocido'
    
    datos['posicion_inferida'] = datos.apply(inferir_posicion, axis=1)
    print(f"   Posiciones inferidas: {datos['posicion_inferida'].value_counts().to_dict()}")
    
    # 3. Manejo de valores nulos por posición
    print("\n3. Tratando valores nulos...")
    nulos_iniciales = datos.isnull().sum().sum()
    
    # Columnas numéricas: mediana por posición
    columnas_numericas = datos.select_dtypes(include=[np.number]).columns
    for columna in columnas_numericas:
        if datos[columna].isnull().any():
            mediana_por_posicion = datos.groupby('posicion_inferida', observed=True)[columna].transform('median')
            datos[columna] = datos[columna].fillna(mediana_por_posicion)
            
            # Si aún hay nulos, usar mediana global
            if datos[columna].isnull().any():
                datos[columna] = datos[columna].fillna(datos[columna].median())
    
    # Columnas categóricas: moda por posición
    columnas_categoricas = datos.select_dtypes(include=['object']).columns
    for columna in columnas_categoricas:
        if columna not in ['date', 'birthday', 'player_name', 'posicion_inferida'] and datos[columna].isnull().any():
            moda_por_posicion = datos.groupby('posicion_inferida', observed=True)[columna].transform(
                lambda x: x.mode()[0] if not x.mode().empty else 'Unknown'
            )
            datos[columna] = datos[columna].fillna(moda_por_posicion)
    
    nulos_finales = datos.isnull().sum().sum()
    print(f"   Nulos eliminados: {nulos_iniciales:,} -> {nulos_finales:,} ({(1 - nulos_finales/nulos_iniciales)*100:.1f}% reducción)")
    
    # 4. Creación de variables derivadas
    print("\n4. Creando variables derivadas...")
    
    # Score físico
    atributos_fisicos = ['acceleration', 'sprint_speed', 'stamina', 'strength']
    atributos_fisicos = [attr for attr in atributos_fisicos if attr in datos.columns]
    if atributos_fisicos:
        datos['score_fisico'] = datos[atributos_fisicos].mean(axis=1)
    
    # Score técnico
    atributos_tecnicos = ['ball_control', 'dribbling', 'short_passing']
    atributos_tecnicos = [attr for attr in atributos_tecnicos if attr in datos.columns]
    if atributos_tecnicos:
        datos['score_tecnico'] = datos[atributos_tecnicos].mean(axis=1)
    
    # Score mental
    atributos_mentales = ['positioning', 'vision', 'reactions']
    atributos_mentales = [attr for attr in atributos_mentales if attr in datos.columns]
    if atributos_mentales:
        datos['score_mental'] = datos[atributos_mentales].mean(axis=1)
    
    # Edad estimada
    if 'birthday' in datos.columns:
        datos['birthday'] = pd.to_datetime(datos['birthday'], errors='coerce')
        fecha_referencia = pd.to_datetime(datos['date']).max() if 'date' in datos.columns else pd.Timestamp('2016-01-01')
        datos['edad_estimada'] = (fecha_referencia - datos['birthday']).dt.days / 365.25
        datos['edad_estimada'] = datos['edad_estimada'].fillna(datos['edad_estimada'].median())
        print(f"   Edad estimada calculada (referencia: {fecha_referencia.date()})")
    else:
        datos['edad_estimada'] = 25
    
    # 5. Codificación one-hot
    print("\n5. Aplicando codificación one-hot...")
    columnas_categoricas_onehot = []
    for columna in datos.select_dtypes(include=['object']).columns:
        if columna not in ['date', 'birthday', 'player_name', 'posicion_inferida'] and datos[columna].nunique() <= 15:
            columnas_categoricas_onehot.append(columna)
    
    if columnas_categoricas_onehot:
        datos_encoded = pd.get_dummies(
            datos[columnas_categoricas_onehot], 
            columns=columnas_categoricas_onehot,
            prefix=columnas_categoricas_onehot,
            dtype=int
        )
        datos = pd.concat([datos.drop(columns=columnas_categoricas_onehot), datos_encoded], axis=1)
        print(f"   Columnas después de one-hot: {datos.shape[1]}")
    
    # 6. Normalización 0-100
    print("\n6. Normalizando atributos numéricos...")
    excluir_normalizacion = ['id', 'player_fifa_api_id', 'player_api_id', 'edad_estimada', 
                            'score_fisico', 'score_tecnico', 'score_mental']
    
    columnas_a_normalizar = [col for col in datos.select_dtypes(include=[np.number]).columns 
                            if col not in excluir_normalizacion]
    
    columnas_normalizadas = 0
    for columna in columnas_a_normalizar:
        try:
            minimo = datos[columna].min()
            maximo = datos[columna].max()
            
            if maximo > minimo:
                datos[columna] = (datos[columna] - minimo) * 100 / (maximo - minimo)
                columnas_normalizadas += 1
            elif maximo == minimo:
                datos[columna] = 50
                columnas_normalizadas += 1
        except:
            continue
    
    print(f"   Columnas normalizadas: {columnas_normalizadas}/{len(columnas_a_normalizar)}")
    
    # 7. Detección y tratamiento de outliers
    print("\n7. Tratando outliers...")
    columnas_clave = ['overall_rating', 'potential', 'score_fisico', 'score_tecnico', 
                     'score_mental', 'crossing', 'finishing', 'short_passing', 
                     'dribbling', 'ball_control']
    
    columnas_existentes = [col for col in columnas_clave if col in datos.columns]
    outliers_tratados = {}
    
    for columna in columnas_existentes:
        try:
            Q1 = datos[columna].quantile(0.25)
            Q3 = datos[columna].quantile(0.75)
            IQR = Q3 - Q1
            
            if IQR > 0:
                limite_inferior = Q1 - 1.5 * IQR
                limite_superior = Q3 + 1.5 * IQR
                
                outliers_count = ((datos[columna] < limite_inferior) | (datos[columna] > limite_superior)).sum()
                
                if outliers_count > 0:
                    datos[columna] = datos[columna].clip(lower=limite_inferior, upper=limite_superior)
                    outliers_tratados[columna] = outliers_count
        except:
            continue
    
    if outliers_tratados:
        for columna, cantidad in outliers_tratados.items():
            porcentaje = (cantidad / len(datos)) * 100
            print(f"   {columna}: {cantidad:,} outliers ({porcentaje:.1f}%)")
    
    # 8. Guardado de resultados
    print("\n8. Guardando resultados...")
    Path(RUTA_SALIDA).parent.mkdir(parents=True, exist_ok=True)
    
    try:
        conn_salida = sqlite3.connect(RUTA_SALIDA)
        datos.to_sql('datos_limpios', conn_salida, if_exists='replace', index=False)
        conn_salida.close()
        
        # Guardar también como CSV
        ruta_csv = RUTA_SALIDA.replace('.sqlite', '.csv')
        datos.to_csv(ruta_csv, index=False)
        
        print(f"   Datos guardados en: {RUTA_SALIDA}")
        print(f"   Copia CSV: {ruta_csv}")
    except Exception as e:
        print(f"   Error al guardar: {e}")
        return
    
    # Resumen final
    print("\n" + "=" * 40)
    print("RESUMEN FINAL:")
    print(f"   Filas: {datos.shape[0]:,}")
    print(f"   Columnas: {datos.shape[1]}")
    print(f"   Valores nulos: {datos.isnull().sum().sum()}")
    
    derivadas = ['score_fisico', 'score_tecnico', 'score_mental', 'edad_estimada']
    creadas = [var for var in derivadas if var in datos.columns]
    print(f"   Variables derivadas: {len(creadas)}/{len(derivadas)} creadas")
    
    if creadas:
        print("   Estadísticas de variables derivadas:")
        for var in creadas:
            stats = datos[var].agg(['min', 'mean', 'max']).round(2)
            print(f"   - {var}: {stats['min']:.1f} | {stats['mean']:.1f} | {stats['max']:.1f}")
    
    print("\nLimpieza completada")
    print("=" * 40)
    
    return datos

if __name__ == "__main__":
    datos_limpios = limpiar_datos_fifa()