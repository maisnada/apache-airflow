import os
import json
import requests
import shutil
import mysql.connector
from os.path import join
from airflow import DAG
from pytz import timezone
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import BranchPythonOperator
import mysql.connector

pasta = 'cotacao'

arquivo = f'dados_brutos_{datetime.today().strftime("%d_%m_%Y")}.json'

dataAtual = (datetime.now().astimezone(timezone('America/Sao_Paulo'))).strftime('%d/%m/%Y %H:%M')

#DAG (Tarefa) iniciará a execução no dia a partir do dia anterior. Ela será executada a cada 30 minutos.
with DAG(
        "cotacao_bitcoin",
        start_date=days_ago(1),
        schedule_interval='*/30 * * * *',
) as dag:  
    
    #cria a pasta de trabalho para salvas os arquivos json
    def criarPasta():

        path = f'/opt/airflow/dags/{pasta}'

        #caso a pasta já exista ela será deletada e recriada
        if os.path.exists(path):
            
            shutil.rmtree(path)
            
        os.mkdir(path)           
      
    #realizar a consulta na API e salva o resultado em um arquivo json (dados_brutos) na pasta de trabalho
    def obterCotacao(): 

        url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest?id=1&convert=BRL'

        headers = {'Accepts': 'application/json', 'X-CMC_PRO_API_KEY': 'XXX'}
        
        response = requests.get(url, headers=headers).json()
        
        with open(f'{os.getcwd()}/dags/{pasta}/{arquivo}', "w") as outfile:
            json.dump(response, outfile)
            
    #abre o json(dados_brutos) e cria um novo arquivo json (filtrar_dados_brutos) somente com a seção de interesse, no caso BRL   
    def filtrarDados(): 
          
        f = open(f'{os.getcwd()}/dags/{pasta}/{arquivo}')  
       
        data = json.load(f)          
       
        f.close()  
        
        with open(f'{os.getcwd()}/dags/{pasta}/filtrar_{arquivo}', "w") as outfile:
            json.dump(data['data']['1']['quote']['BRL'], outfile)
            
    #abre o json (filtrar_dados_brutos) e extrai a propriedade price que corresponde ao preco do Bitcoin        
    def extracaoDados(): 
        
        f = open(f'{os.getcwd()}/dags/{pasta}/filtrar_{arquivo}')  
       
        data = json.load(f)          
       
        f.close()
        
        #a prorpiedade price é convertida de string para float e o valor é arredondado para contem apenas 2 casas decimais
        preco = round(float(data['price']),2)         
        
        #utilizamos o mecanimos XComs do AirFlow para salvar o preco e data atual. XComs permite a troca de dados entre as tarefas 
        return {"preco":preco,"dataAtualizacao":dataAtual} 
            
    #recupera o preco e data e salva no banco de dados Mysql   
    def persistirDados(ti):
        
        #utilizamos o XComs para obter os dados salvo na tarefa extracao_dados
        extracao = ti.xcom_pull(task_ids='extracao_dados') 
        
        preco = float(extracao["preco"]) 
        
        dataAtualizacao = extracao["dataAtualizacao"]
        
        conexao = mysql.connector.connect(
        host="XXX",
        port=XXX,
        user="XXX",
        password="XXX",
        database="XXX"
        )

        cursor = conexao.cursor()

        sql = "INSERT INTO cotacao (valor, data_atualializacao) VALUES (%s, %s)"
        
        valores = (preco, dataAtualizacao)

        cursor.execute(sql, valores)

        conexao.commit() 
            
    #conficional que retornará a próxima tarefa a ser executada com base no preco  
    def verificaValor(ti):
         
        extracao = ti.xcom_pull(task_ids='extracao_dados') 
        
        print(extracao)
        
        preco = float(extracao["preco"])             
        
        if preco < 130000:
                       
            return 'enviar_notificao'
        
        else:
            
            return 'nao_enviar_notificao'    
    
    #realzia o envio da notificação via e-mail através da API SendGrid    
    def enviarNotificao(ti):
        
        extracao = ti.xcom_pull(task_ids='extracao_dados') 
        
        preco = float(extracao["preco"]) 
                
        url = 'https://api.sendgrid.com/v3/mail/send'

        headers = {'Authorization': 'Bearer XXX', 'Content-Type': 'application/json'}
        
        titulo = f'Alerta Bitcoin ;D'
        
        conteudo = f'<br/>O Valor do Bitcoin é <b>R$ {preco}</b>!'

        data = {"personalizations": [{"to": [{"email": "maisnada@gmail.com"}]}],"from": {"email": "admin@botecoscore.com.br"},"subject": titulo,"content": [{"type": "text/html", "value": conteudo}]} 

        response = requests.post(url, headers = headers, json = data)

        print(response.status_code)        
            
    #relação das atividades que compoem a tarefa (DAG)
    
    #operdador PythonOperator permite excutar código Python encapsulado em funções (def)    
    cria_pasta = PythonOperator(
        task_id = 'cria_pasta',
        python_callable = criarPasta       
    )
        
    obter_cotacao = PythonOperator(
        task_id = 'obter_cotacao',
        python_callable = obterCotacao
    )
    
    filtrar_dados = PythonOperator(
        task_id = 'filtrar_dados',
        python_callable = filtrarDados
    )    
    
    extracao_dados = PythonOperator(
        task_id = 'extracao_dados',
        python_callable = extracaoDados,
        do_xcom_push=True
    )
        
    persistir_dados = PythonOperator(
        task_id = 'persistir_dados',
        python_callable = persistirDados
    )
    
    #operador BranchPythonOperator permite ramificar uma tarefa e permite o retorno da próxima tarefa a ser executada
    verificar_valor = BranchPythonOperator(
        task_id='verificar_valor',
        python_callable=verificaValor)    
    
    enviar_notificao = PythonOperator(
        task_id='enviar_notificao',
        python_callable=enviarNotificao)           
     
    nao_enviar_notificao = DummyOperator(task_id = 'nao_enviar_notificao')
     
    #relacionamento e sequencia das atividades a serem execuatdas
    cria_pasta >> obter_cotacao >> filtrar_dados >> extracao_dados >> persistir_dados >> verificar_valor >>  [enviar_notificao, nao_enviar_notificao]  