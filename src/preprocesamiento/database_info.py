# src/preprocesamiento/database_info.py
import sqlite3
import pandas as pd

class DatabaseAnalyzer:
    """Analizador de estructura de base de datos FIFA"""
    
    def __init__(self, db_path="data/raw/database.sqlite"):
        self.db_path = db_path
        self.connection = None
        
    def connect(self):
        """Establece conexión con la base de datos"""
        try:
            self.connection = sqlite3.connect(self.db_path)
            print(f"Conexión exitosa a: {self.db_path}")
            return True
        except Exception as e:
            print(f"Error al conectar: {e}")
            return False
    
    def get_all_tables(self):
        """Obtiene lista de todas las tablas en la base de datos"""
        if not self.connection:
            return []
        
        query = "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;"
        tables_df = pd.read_sql_query(query, self.connection)
        return tables_df['name'].tolist()
    
    def analyze_table_structure(self, table_name):
        """Analiza la estructura de una tabla específica"""
        if not self.connection:
            return None
        
        # Información de columnas
        cursor = self.connection.cursor()
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns_info = cursor.fetchall()
        
        # Contar filas
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]
        
        # Obtener muestra de datos
        sample_query = f"SELECT * FROM {table_name} LIMIT 3"
        sample_data = None
        try:
            sample_data = pd.read_sql_query(sample_query, self.connection)
        except:
            sample_data = None
        
        return {
            'table_name': table_name,
            'column_count': len(columns_info),
            'row_count': row_count,
            'columns': columns_info,
            'sample': sample_data
        }
    
    def find_fifa_attributes(self):
        """Busca columnas relevantes para análisis FIFA"""
        if not self.connection:
            return {}
        
        tables = self.get_all_tables()
        fifa_attributes = {}
        
        # Lista de atributos FIFA que buscamos
        target_attributes = [
            'overall', 'potential', 'rating', 'age', 'position',
            'acceleration', 'speed', 'stamina', 'strength',
            'control', 'dribbling', 'passing', 'crossing',
            'finishing', 'positioning', 'vision', 'reaction',
            'shot', 'defense', 'physic'
        ]
        
        for table in tables:
            cursor = self.connection.cursor()
            cursor.execute(f"PRAGMA table_info({table})")
            columns = cursor.fetchall()
            
            found_columns = []
            for col in columns:
                col_name = col[1].lower()
                for attr in target_attributes:
                    if attr in col_name:
                        found_columns.append(col[1])
                        break
            
            if found_columns:
                fifa_attributes[table] = found_columns
        
        return fifa_attributes
    
    def generate_report(self):
        """Genera un reporte completo de la base de datos"""
        if not self.connect():
            return
        
        print("=" * 80)
        print("INFORME DE ESTRUCTURA - BASE DE DATOS FIFA")
        print("=" * 80)
        
        # 1. Listar tablas
        tables = self.get_all_tables()
        print(f"\n1. TABLAS DISPONIBLES ({len(tables)}):")
        print("-" * 40)
        for i, table in enumerate(tables, 1):
            print(f"   {i:2d}. {table}")
        
        # 2. Análisis detallado de cada tabla
        print("\n2. ANÁLISIS DETALLADO POR TABLA:")
        print("-" * 40)
        
        for table in tables:
            analysis = self.analyze_table_structure(table)
            if analysis:
                print(f"\n   TABLA: {analysis['table_name']}")
                print(f"   • Filas: {analysis['row_count']:,}")
                print(f"   • Columnas: {analysis['column_count']}")
                
                # Mostrar columnas principales (primeras 10)
                if analysis['columns']:
                    print(f"   • Columnas principales:")
                    for i, col in enumerate(analysis['columns'][:10], 1):
                        col_id, col_name, col_type, notnull, default_val, pk = col
                        print(f"     {i:2d}. {col_name:25} ({col_type:10})")
                    
                    if len(analysis['columns']) > 10:
                        print(f"     ... y {len(analysis['columns'])-10} columnas más")
                
                # Mostrar tipo de datos de la tabla
                if analysis['sample'] is not None and not analysis['sample'].empty:
                    print(f"   • Tipos de datos (primeras 5 columnas):")
                    for col in analysis['sample'].columns[:5]:
                        dtype = str(analysis['sample'][col].dtype)
                        print(f"     • {col:20}: {dtype}")
        
        # 3. Atributos FIFA encontrados
        print("\n3. ATRIBUTOS FIFA IDENTIFICADOS:")
        print("-" * 40)
        
        fifa_attrs = self.find_fifa_attributes()
        if fifa_attrs:
            for table, attributes in fifa_attrs.items():
                print(f"\n   Tabla '{table}':")
                for attr in attributes[:15]:  # Mostrar máximo 15 por tabla
                    print(f"     • {attr}")
                if len(attributes) > 15:
                    print(f"     ... y {len(attributes)-15} atributos más")
        else:
            print("   No se encontraron atributos FIFA con nombres típicos")
        
        # 4. Recomendaciones para el proyecto
        print("\n4. RECOMENDACIONES PARA EL PROYECTO:")
        print("-" * 40)
        
        # Identificar tablas clave
        player_tables = [t for t in tables if 'player' in t.lower()]
        attribute_tables = [t for t in tables if 'attribute' in t.lower()]
        
        if player_tables:
            print("   • Tablas de jugadores identificadas:")
            for table in player_tables:
                print(f"     - {table}")
        
        if attribute_tables:
            print("   • Tablas de atributos identificadas:")
            for table in attribute_tables:
                print(f"     - {table}")
        
        # Recomendación principal
        print("\n   • TABLA PRINCIPAL RECOMENDADA:")
        if 'Player_Attributes' in tables:
            print("     Player_Attributes - Contiene ratings y estadísticas de jugadores")
            print("     (183,978 filas × 42 columnas)")
        
        print("\n" + "=" * 80)
        print("FIN DEL INFORME")
        print("=" * 80)
        
        self.connection.close()

def main():
    """Función principal para ejecutar el análisis"""
    analyzer = DatabaseAnalyzer()
    analyzer.generate_report()

if __name__ == "__main__":
    main()