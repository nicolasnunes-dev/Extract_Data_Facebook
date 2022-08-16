import requests
import json
import pandas as pd
from datetime import date

def main():
    
    #token facebook developer e account id da conta Data GhFly
    fb_token_api = '' #Facebook token api in Meta for Developers
    account_id = '' #Account ID Meta for Developers
    
    result = request_json(fb_token_api=fb_token_api, account_id=account_id)
    
    dataframe = df_construct()
    
    #Váriavel para pegar a data de hoje.
    today = date.today()

    #loop campanha
    loop_campaign = 0;

    #loop do cliente
    loop_adaccount = 0;

    #seta primeira ad account
    name_adaccount = result['adaccounts']['data'][loop_adaccount]['name']

    #while dos clientes e valida se a ad account (conta do cliente)
    while name_adaccount != None:
        
        #tentativa de pegar a primeira campanha se não conseguir seta como nulo
        try:
            name_campaign = result['adaccounts']['data'][loop_adaccount]['campaigns']['data'][loop_campaign]['name']
        except:
            name_campaign = None

        #while do nome da campnha, como são várias campanhas dentro de um cliente ele pega um a um, valida se a campanha
        while name_campaign != None:
            
            #valida se a campanha tem insights cpc, spend, impressions. Se não tiver ele passa para próxima campanha sem adicionar ela ao DF
            try:    
                insight_campaign = result['adaccounts']['data'][loop_adaccount]['campaigns']['data'][loop_campaign]['insights']['data'][0]
            except:
                insight_campaign = None

            #Se não tiver ele passa para próxima campanha sem adicionar ela ao DF
            if insight_campaign != None:
                
                #seta as variaveis com as informações dos clientes e insights do json
                nome_conta = result['adaccounts']['data'][loop_adaccount]['name']
                nome_campanha = result['adaccounts']['data'][loop_adaccount]['campaigns']['data'][loop_campaign]['name']
                id_conta = result['adaccounts']['data'][loop_adaccount]['campaigns']['data'][loop_campaign]['account_id']
                criado = result['adaccounts']['data'][loop_adaccount]['campaigns']['data'][loop_campaign]['created_time']
                try:
                    id_campanha = clicks = result['adaccounts']['data'][loop_adaccount]['campaigns']['data'][loop_campaign]['insights']['data'][0]['campaign_id']
                except:
                    id_campanha = None
                try:
                    clicks = result['adaccounts']['data'][loop_adaccount]['campaigns']['data'][loop_campaign]['insights']['data'][0]['clicks']
                except:
                    clicks = 0
                try:
                    cpc =  result['adaccounts']['data'][loop_adaccount]['campaigns']['data'][loop_campaign]['insights']['data'][0]['cpc']
                except:
                    cpc = 0
                try:    
                    impressions = result['adaccounts']['data'][loop_adaccount]['campaigns']['data'][loop_campaign]['insights']['data'][0]['impressions']
                except:
                    impressions = 0
                try:
                    spend = result['adaccounts']['data'][loop_adaccount]['campaigns']['data'][loop_campaign]['insights']['data'][0]['spend']
                except:
                    spend = 0
                try:
                    data_inicio = result['adaccounts']['data'][loop_adaccount]['campaigns']['data'][loop_campaign]['insights']['data'][0]['date_start']
                except:
                    data_inicio = 0
                try:
                    data_parada = result['adaccounts']['data'][loop_adaccount]['campaigns']['data'][loop_campaign]['insights']['data'][0]['date_stop']
                except:
                    data_parada = 0

                #adiciona nova linha com as informações da campanha dentro do DF
                new_row = {'Data_insercao': today.strftime("%d/%m/%Y"),
                        'Nome_Conta': nome_conta,
                        'ID_Conta': id_conta, 
                        'Nome_Campanha': nome_campanha, 
                        'ID_Campanha': id_campanha, 
                        'Criado': criado, 
                        'Clicks': clicks, 
                        'Cpc': cpc, 
                        'Impressions': impressions,
                        'Spend': spend,
                        'Data_Inicio': data_inicio,
                        'Data_Parada': data_parada
                        }
                
                dataframe = dataframe.append(new_row, ignore_index=True)
                
                #contador campanha
                loop_campaign = loop_campaign + 1
                
                #seta nome da proxima campanha do mesmo cliente
                try:
                    name_campaign = result['adaccounts']['data'][loop_adaccount]['campaigns']['data'][loop_campaign]['name']
                except:
                    name_campaign = None
            
            #Se não houver insights, adiciona +1 no loop da campanha para irmos a próxima e seta nome da próxima campanha                         
            else:
                
                loop_campaign = loop_campaign + 1
                try:
                    name_campaign = result['adaccounts']['data'][loop_adaccount]['campaigns']['data'][loop_campaign]['name']
                except:
                    name_campaign = None
        
        #quando não há mais camapanhas sai deste while, seta +1 neste loop, seta o nome da próxima ad account e zera o contador da campanha.       
        if name_campaign == None:
            loop_adaccount = loop_adaccount + 1
            loop_campaign = 0;
                
            try:
                name_adaccount = result['adaccounts']['data'][loop_adaccount]['name']
            except:
                name_adaccount = None

    #df.dtypes

    dataframe[["Data_insercao", "Data_Inicio", "Data_Parada"]] =  dataframe[["Data_insercao", "Data_Inicio", "Data_Parada"]].apply(pd.to_datetime) 
    dataframe[["Clicks", "Cpc", "Impressions", "Spend"]] =  dataframe[["Clicks", "Cpc", "Impressions", "Spend"]].apply(pd.to_numeric)

    return dataframe  

def request_json(fb_token_api, account_id):

    #url base para a requisição, os campos que vão ser solicitados e concatena no final com o token de autenticação.
    base_url = "https://graph.facebook.com/v14.0/"      
    fields = ["id,name,adaccounts{name,account_id,account_status,campaigns{name,account_id,created_time,insights{clicks,cpc,campaign_id,impressions,spend},spend_cap}}"]
    token = "&access_token=" + fb_token_api

    #adiciona a account id e os campos na url de requisição
    url = base_url + str(account_id)
    url += "?fields=" + ",".join(fields)

    #Faz a requisição ao Facebook
    result = requests.get(url + token)

    #Carrega JSON e seta decode UTF-8 (padrão de letras)
    result = json.loads(result._content.decode("utf-8"))
    return result

def df_construct():
    #Monta o dataframe com as colunas, seta a tipagem das colunas e aplica
    df = pd.DataFrame(columns=['Data_insercao','Nome_Conta', 'ID_Conta', 'Nome_Campanha', 'ID_Campanha', 'Criado', 'Clicks', 'Cpc', 'Impressions', 'Spend', 'Data_Inicio', 'Data_Parada'])

    datatypes_per_column = {
        "Data_insercao": "datetime64[ms]",
        "Nome_Conta": "string",
        "ID_Conta": "string",
        "Nome_Campanha": "string",
        "ID_Campanha": "string",
        "Criado": "string",
        "Clicks": "float64",
        "Cpc": "float64",
        "Impressions": "float64",
        "Spend": "float64",
        "Data_Inicio": "datetime64[ms]",
        "Data_Parada": "datetime64[ms]"
        }
    
    df = df.astype(datatypes_per_column)
    return df


dataframe_final = main()
dataframe_final.to_csv('teste.csv')  
