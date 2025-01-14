import psycopg2
import json
import pandas as pd
from sqlalchemy import create_engine

hostname = 'localhost'
database = 'Company_Set'
username = 'postgres'
pwd = '250857'
port_id = 5432
conn = None
cur = None

try:
    conn = psycopg2.connect(
        host=hostname,
        dbname=database,
        user=username,
        password=pwd,
        port=port_id
    )
    cur = conn.cursor()

    def etl_process(json_file,postgres_uri,table_name):
        with open(json_file,'r') as file:
            data = json.load(file)
        
        df = pd.json_normalize(data,errors = 'ignore')

        engine = create_engine(postgres_uri)

        df.to_sql(table_name,engine,if_exists='replace',index=False)

        print(f"ETL process completed. Data loaded into '{table_name} table")

    json_file1_path = r"C:\Users\davpt\Downloads\authors_data_large.json"
    json_file2_path = r"C:\Users\davpt\Downloads\publishers_data_large.json"
    json_file3_path = r"C:\Users\davpt\Downloads\books_data_large.json"
    postgres_uri = 'postgresql://postgres:250857@localhost:5432/Company_Set'
    table1_name = 'q4_csv'
    table2_name = 'q5_csv'
    table3_name = 'q6_csv'

    etl_process(json_file1_path,postgres_uri,table1_name)
    etl_process(json_file2_path,postgres_uri,table2_name)
    etl_process(json_file3_path,postgres_uri,table3_name)

    alter_author_table_script = ''' ALTER TABLE q4_csv
                                    ADD CONSTRAINT author_id UNIQUE(author_id)'''
    
    cur.execute(alter_author_table_script)

    alter_publisher_table_script = ''' ALTER TABLE q5_csv
                                       ADD CONSTRAINT publisher_id UNIQUE(publisher_id)'''
    
    cur.execute(alter_publisher_table_script)

    relation_script1 = ''' ALTER TABLE q6_csv
                          ADD CONSTRAINT FK_1
                          FOREIGN KEY (book_id)
                          REFERENCES q4_csv(author_id)
                          ON DELETE CASCADE
                          ON UPDATE CASCADE'''
    
    cur.execute(relation_script1)
    print("FK_1 added")
    relation_script2 = ''' ALTER TABLE q6_csv
                           ADD CONSTRAINT FK_2
                           FOREIGN KEY (publisher_id)
                           REFERENCES q5_csv(publisher_id)
                           ON DELETE CASCADE
                           ON UPDATE CASCADE'''
    
    cur.execute(relation_script2)
    print("FK_2 added")


    insert_script1 = ''' INSERT INTO q4_csv(author_id,name)
                        VALUES
                        (10001,'Akshat');'''
    cur.execute(insert_script1)
    
    print("script1 added")

    insert_script2 = ''' INSERT INTO q5_csv(publisher_id,name)
                         VALUES
                         (10002,'Akshat')'''
    cur.execute(insert_script2)
    
    print("script2 added")

    insert_script3 = ''' INSERT INTO q6_csv(book_id,title,publisher_id)
                         VALUES
                         (10001,'halfbloods',10002)'''
    cur.execute(insert_script3)
    conn.commit()




except Exception as error:
    print("Error:", error)

finally:

    if cur is not None:
        cur.close()
    if conn is not None:
        conn.close()
