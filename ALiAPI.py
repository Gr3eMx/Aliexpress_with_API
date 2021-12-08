import pymysql
import json
from datetime import datetime,timedelta
from aliexpress import api,appinfo
from bs4 import BeautifulSoup
url = 'gw.api.taobao.com'
port = 80
appkey = ''
secret = ''
sessionkey = ''
sku_id = ['']
now = datetime.now().date()
dateFrom = str(now - timedelta(days=1))

def get_sells():
     req = api.AliexpressSolutionOrderGetRequest(url, port)
     req.set_app_info(appinfo(appkey, secret))

     req.param0 = {
     'create_date_start': (datetime.now() - timedelta(1)).strftime('%Y-%m-%d') + ' 00:00:01',
     'create_date_end': (datetime.now() - timedelta(1)).strftime('%Y-%m-%d') + ' 24:00:00',
     'page_size': 50,
     'current_page': 1
     }
     resp = req.getResponse(sessionkey)
     with open('dsa.json','w', encoding='utf-8') as file:
          json.dump(resp, file, indent=4, ensure_ascii=False)

     with open('dsa.json', 'r', encoding='utf-8') as file:
          file_content = file.read()
     soup = BeautifulSoup(file_content, 'html.parser')
     site_json = json.loads(soup.text)
     data_sells_count = {}
     data_sells_rub = {}
     try:
          for i in site_json['aliexpress_solution_order_get_response']['result']['target_list']['order_dto']:
               for z in i['product_list']['order_product_dto']:
                    if z['product_id'] not in data_sells_rub:
                         data_sells_rub[z['product_id']] = float(z['total_product_amount']['amount'])
                    else:
                         data_sells_rub[z['product_id']] += float(z['total_product_amount']['amount'])
                    if z['product_id'] not in data_sells_count:
                         data_sells_count[z['product_id']] = z['product_count']
                    else:
                         data_sells_count[z['product_id']] += z['product_count']

     except:
          print('Заказов вчера не было')
     for z in sku_id:
          if z not in data_sells_rub:
               data_sells_rub[z] = 0
          if z not in data_sells_count:
               data_sells_count[z] = 0
     return data_sells_count, data_sells_rub
def data_stock():
     stick = []
     req2 = api.AliexpressSolutionProductInfoGetRequest(url, port)
     req2.set_app_info(appinfo(appkey, secret))
     for i in sku_id:
          req2.product_id= i
          resp2 = req2.getResponse(sessionkey)
          stick.append(resp2)
     with open('dsa2.json','w', encoding='utf-8') as file:
          json.dump(stick, file, indent=4, ensure_ascii=False)

     with open('dsa2.json', encoding='utf-8') as file:
         conten = file.read()
     soup = BeautifulSoup(conten, 'html.parser')
     site_json = json.loads(soup.text)
     value_count_sklad = []
     for i in site_json:
         for z in i['aliexpress_solution_product_info_get_response']['result']['aeop_ae_product_s_k_us']['global_aeop_ae_product_sku']:
             value_count_sklad.append(z['ipm_sku_stock'])
     values_id = dict(zip(sku_id,value_count_sklad))

     return values_id
def scraping_info():
     sells_count = get_sells()[0]
     sells_sum = get_sells()[1]
     stock = data_stock()
     slu = []
     for i in sku_id:
          slu.append({
               'sku_id': str(i),
               'sells_count': sells_count[i],
               'sells_sum': sells_sum[i],
               'stock': stock[i]

          })
     with open('json_ali.json', 'w', encoding='utf-8') as file:
          json.dump(slu,file ,ensure_ascii=4, indent=False)
def insert_DB():
     connection = pymysql.connect('')
     with open('json_ali.json', encoding='utf-8') as file:
          file_content = file.read()
     soup = BeautifulSoup(file_content, 'html.parser')
     site_json = json.loads(soup.text)
     totals_count = 0
     totals_pr_rub = 0
     totals_stock = 0
     for c in site_json:
          totals_count += float(c['sells_count'])
          totals_pr_rub += float(c['sells_sum'])
          totals_stock += (c['stock'])

     with connection.cursor() as cursor:
          cursor.execute('SELECT sku_id, main_id FROM main_aliexpress1 WHERE is_published = 1')
          id_mains_sku = dict(cursor.fetchall())
     for i in site_json:
          # try:
          #      with connection.cursor() as cursor:
          #           cursor.execute(
          #                f"CREATE TABLE aliexpress1_{id_mains_sku[i['sku_id']]}(ID int NOT NULL AUTO_INCREMENT, date varchar(25) NOT NULL,  pr varchar(25) NOT NULL, pr_rub varchar(25) NOT NULL, stock varchar(25) NOT NULL, refund varchar(25) NOT NULL, cancel varchar(25) NOT NULL, conversion varchar(25) NOT NULL, PRIMARY KEY (ID))")
          # except Exception as ex:
          #      print('Не получилось создать')
          #      print(ex)
          try:
               with connection.cursor() as cursor:
                    cursor.execute(f'SELECT date,pr FROM aliexpress1_{id_mains_sku[i["sku_id"]]}')
                    date_last = dict(cursor.fetchall())
               if dateFrom not in date_last:
                    with connection.cursor() as cursor:
                         cursor.execute(
                              f"INSERT INTO aliexpress1_{id_mains_sku[i['sku_id']]} (date,pr,pr_rub,stock,refund,cancel,conversion) VALUES ('{dateFrom}', {i['sells_count']}, {i['sells_sum']}, {i['stock']}, 0, 0, 0)")
               else:
                    continue
          except Exception as ex:
               print('Не получилось добавить')
               print(ex)
     # try:
     #      with connection.cursor() as cursor:
     #           cursor.execute(
     #                f"CREATE TABLE aliexpress1_totals (ID int NOT NULL AUTO_INCREMENT, date varchar(25) NOT NULL,  pr varchar(25) NOT NULL, pr_rub varchar(25) NOT NULL, stock varchar(25) NOT NULL, refund varchar(25) NOT NULL, cancel varchar(25) NOT NULL, conversion varchar(25) NOT NULL, turn_enemy varchar(25) NOT NULL, PRIMARY KEY (ID))")
     # except Exception as ex:
     #      print('Не создал таблицу')
     #      print(ex)
     try:
          with connection.cursor() as cursor:
               cursor.execute(f'SELECT date,pr FROM aliexpress1_totals')
               date_last1 = dict(cursor.fetchall())
          if dateFrom not in date_last1:
               with connection.cursor() as cursor:
                    cursor.execute(
                         f"INSERT INTO aliexpress1_totals (date,pr,pr_rub,stock,refund,cancel,conversion,turn_enemy) VALUES ('{dateFrom}', {totals_count}, {totals_pr_rub}, {totals_stock}, 0, 0, 0, 0 )")
          connection.commit()
     except Exception as ex:
          print('Не получилось засунуть данные в таблицу')
          print(ex)
     finally:
          connection.close()

def main():
     scraping_info()
     insert_DB()
if __name__ == "__main__":
    main()
