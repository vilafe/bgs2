import asyncio, psycopg2
from os import close
import asyncpg
import paramiko, os


def iptables():
# Блокировака соединения между машинами
    host = 'sql1.lab.local'
    user = 'root'
    secret = '34650qqQQ'
    port = 22

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(hostname=host, username=user, password=secret, port=port)
    stdin, stdout, stderr = client.exec_command('sh /home/stop_network.sh')
    client.close()    

def check_ping(hostname):
    response = os.system("ping " + hostname)
    return response


def compare():
    test = 0
    while check_ping("sql1.lab.local") == 0:
        con = psycopg2.connect(dbname='test_rlpk2', user='lexa', 
                            password='lexa', host='sql1.lab.local')
        cur = con.cursor()
        cur.execute('SELECT COUNT(*) FROM pushes;')
        count = cur.fetchone()
        con2 = psycopg2.connect(dbname='test_rlpk2', user='lexa', 
                            password='lexa', host='sql2.lab.local')
        cur2 = con2.cursor()
        cur2.execute('SELECT COUNT(*) FROM pushes;')
        count2 = cur2.fetchone()

        print(f"В первой БД - {count[0]} записей.\nВо второй БД - {count2[0]} записей.")
        if count[0] == count2[0]:
            print("Ошибок не найдено")
            exit(0)
        if count[0] != count2[0]:
            print(f"Найдены ошибки, количество неверных записей - {abs(count[0]-count2[0])}")
            exit(0)


async def run():
    conn = await asyncpg.connect(database='test_rlpk2', user='lexa', 
                        password='lexa', host='sql1.lab.local', command_timeout='2')
    try:
        i = 0
        while i < 1000:
            async with conn.transaction():
                await conn.execute(f"INSERT INTO pushes (id, time) VALUES ('{i}', 'now()');")
            print (i)
            if i == 500:
                iptables()
            i = i + 1
    except:
        pass

#cursor.execute('CREATE TABLE pushes (id int, time time);')

loop = asyncio.get_event_loop()
loop.run_until_complete(run())


compare()