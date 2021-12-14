#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import pandas as pd
import sys
from bs4 import BeautifulSoup

def get_zendesk_macros_page(
        user, token, url='https://muunwallet.zendesk.com/api/v2/macros.json'):
    '''
    Descargar automaticamente macros de zendesk en un dado url.
    '''
    command = f'curl {url:s} \  -u {user:s}/token:{token:s}'
    stream = os.popen(command)
    stream = stream.read()
    macros = json.loads(stream)
    return macros

def update_macros(user, token, output_file='macros_principal.csv'):
    '''
    Descargar todos los macros de zendesk.
    
    Descarga todos los macros de Zendesk, generando un DataFrame con datos
    relevantes de los mismos. Guarda todo en formato CSV.
    
    Parameters
    ----------
    user : string
        Mail del usuario de zendesk.
    token : string
        Token para autenticacion.

    Returns
    -------
    macros : pandas DataFrame
        dataframe con todos los macros guardados en Zendesk

    '''
    lista_dfs = []
    url = 'https://muunwallet.zendesk.com/api/v2/macros.json'
    while url is not None:
        macros = get_zendesk_macros_page(
                    url=url,
                    user=user,
                    token=token
                    )
        lista_dfs.append(zendesk_macros_json_to_df(macros['macros']))
        url = macros['next_page']
    
    # parseo la columna html de cada dataframe a texto
    for df in lista_dfs:
      df.dropna(subset=['title'], inplace=True) 
      formatear_columna_html_a_text(df)
      print('nuevo parseo')
    
    df_concatenado = pd.concat(lista_dfs, axis=0)
    
    # guardo el df en formato csv
    df_concatenado.to_csv(os.path.join('.', output_file))
    return df_concatenado
    

def zendesk_macros_json_to_df(json_macros):

  campos = [
      'title',
      'updated_at',
      #'created_at',
      'actions'
      ]

  subcampos = [
      'comment_value_html',
      'subject'
      ]

  df = []

  for entrada in json_macros:
      entrada = {campo : entrada[campo] for campo in campos}
      actions = entrada.pop('actions')
      for action in actions:
          for subcampo in subcampos:
              if subcampo in action['field']:
                  entrada[subcampo] = action['value']
      df.append(entrada)
  df = pd.DataFrame(df).infer_objects()
  return df

def formatear_columna_html_a_text (df, columna='comment_value_html'):
  '''Cambiar formato html a texto estandar para una columna de un dataframe'''

  df[columna].fillna('N/A', inplace=True)  
  lista_aux = []
  for fila in df[columna]:
    soup = BeautifulSoup(fila, features='html.parser')
    text = soup.get_text('\n')
    lista_aux.append(text)
  df[columna] = lista_aux
  df[columna].fillna('N/A', inplace=True)





#%% main
if __name__ == '__main__':
    if len(sys.argv) == 3:
        _, user, token = sys.argv
        update_macros(user, token)
    elif len(sys.argv) == 4:
        _, user, token, output_file = sys.argv
        update_macros(user, token, output_file)
    else:
        raise Exception('Uso correcto: update_macros user token [output_file]')