"""
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.

1. Nombres de las columnas en minúsculas
2. Nombres de las columnas remplazando los espacios por guiones bajos
3. las palabras clave deben estar separadas por comas y con un solo espacio entre palabra y palabra


"""
import pandas as pd


def ingest_data():

    #
    # Inserte su código aquí
    #
    filename = 'clusters_report.txt'
    with open(filename, 'r') as file:
        lines_df = file.readlines()
    
    # Crear una lista
    datos = []
      
    # Iteración sobre los elementos de la lista
    for line in lines_df[4:]:
        
        '''
        Dentro del iterador for se eliminaran todos los espacios en
        blanco al inicio y final de cada caracter (recordar que las líneas
        son strings o cadena de caracteres)
        '''
        word = line.strip() # ---> Elimina caracteres específicos (por defecto se eliminan los espacios en blanco al inicio y al final de una cadena de caracteres)
        #print(word)
        '''
        Cada fila (string) la convierte en una lista cuyo 
        separador son los espacios en blanco
        '''
        word = word.split() 
        #print(word)
        
        '''
        Realiza un testeo de la lineas entrando a un condicional el cual 
        pregunta si la longitud de la lista (linea de texto) es mayor que cero
        y a vez si el elemento cero de cada linea es un dígito
        '''
        
        if len(word) > 0 and word[0].isdigit():
            
            cluster = int(word[0])
            cantidad_de_palabras_clave = int(word[1])
            porcentaje_de_palabras_clave = float(word[2].replace(',','.'))
            principales_palabras_clave = ' '.join(word[3:]).replace(',',', ').replace('  ',' ').replace('% ','').replace('   ',' ').replace('    ',' ').replace('.','')
            principales_palabras_clave = principales_palabras_clave.strip()
            datos.append({'cluster':cluster, 'cantidad_de_palabras_clave':cantidad_de_palabras_clave,
                        'porcentaje_de_palabras_clave':porcentaje_de_palabras_clave, 'principales_palabras_clave':principales_palabras_clave})
            
        elif len(word) > 0:
       
            principales_palabras_clave = ' '.join(word[0:]).replace(',',', ').replace('  ',' ').replace('% ','').replace('   ',' ').replace('    ',' ').replace('.','')
            principales_palabras_clave = principales_palabras_clave.strip()
            datos.append({'cluster':cluster, 'cantidad_de_palabras_clave':cantidad_de_palabras_clave,
                        'porcentaje_de_palabras_clave':porcentaje_de_palabras_clave, 'principales_palabras_clave':principales_palabras_clave})    

            
    df = pd.DataFrame(datos)    
    df = df.groupby(['cluster','cantidad_de_palabras_clave','porcentaje_de_palabras_clave'])['principales_palabras_clave'].agg(lambda x: ' '.join(x.astype(str))).reset_index()
    
    return df
