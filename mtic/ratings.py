import psycopg2
from psycopg2.extras import execute_values

def getOpenConnection(user='test',password='test',dbname='testing'):
   conn_string="host='localhost' dbname='"+dbname+"' user='"+user+"' password='"+password+"'"
   return psycopg2.connect(conn_string)

def createTable(nombretabla, conn):
   cur = conn.cursor()
   comando='drop table if exists '+nombretabla
   cur.execute(comando)
   comando='create table '+nombretabla+' (userid int, filler1 char(1), movieid int, filler2 char(1), rating float, filler3 char(1), marca int)'
   cur.execute(comando)
   cur.close()

def dropTableFillerCols(nombretabla, conn):
   cur = conn.cursor()
   comando = 'alter table '+nombretabla+' drop column filler1, drop column filler2, drop column filler3'
   cur.execute(comando)
   cur.close()

def loadRatings(nombretabla,nombrearchivo,conn):
   cur = conn.cursor()
   createTable(nombretabla,conn)
   with open(nombrearchivo, 'r') as f:
       cur.copy_from(f, nombretabla, sep=':')
   f.close() 
   dropTableFillerCols(nombretabla,conn)
   cur.close()

def crearTablaPart(nombretabla,conn):
   cur = conn.cursor()
   comando='drop table if exists '+nombretabla
   cur.execute(comando)
   comando = 'create table '+nombretabla+' (userid int, movieid int, rating float, marca int)'
   cur.execute(comando)
   cur.close()
   

def particionRango(nombretabla, cantpart, conn):
   cur = conn.cursor()
   cur.execute('select count(*) from '+nombretabla)
   totalfilas = cur.fetchone()
   # Calcular filas p/cada particion
   filas = totalfilas[0] / cantpart
   resto = totalfilas[0] % cantpart
   print 'La tabla '+nombretabla+' se dividira en '+str(cantpart)+' particiones de '+str(filas)+' filas c/u y un resto de '+str(resto)+' filas'
   cur.execute('select * from '+nombretabla)
   inscmd = ''
   for i in range(1,cantpart+1):
      reg = cur.fetchmany(filas)
      nombretablapart = nombretabla+'_'+str(i)
      crearTablaPart(nombretablapart,conn)
      inscmd = 'insert into '+nombretablapart+' values %s'
      execute_values(conn.cursor(),inscmd,reg)
   reg = cur.fetchmany(resto)
   execute_values(conn.cursor(),inscmd,reg)
   cur.close()

def particionRoundRobin(nombretabla,cantpart,conn):
   # Crear particiones
   for i in range(0,cantpart):
      nombretablapart = nombretabla+'_'+str(i+1)
      crearTablaPart(nombretablapart,conn)
   # Poblar particiones
   cur = conn.cursor()
   cur.execute('select * from '+nombretabla)
   nropart = 0
   while True:
      reg = cur.fetchone()
      if reg == None:
         break
      inscmd='insert into '+nombretabla+'_'+str(nropart+1)+' values (%s,%s,%s,%s)'
      conn.cursor().execute(inscmd,reg)
      if nropart+1 == cantpart:
         nropart = 0
      else:
         nropart += 1
   cur.close()

conn = getOpenConnection()
loadRatings('ratings','testing.dat',conn)
#particionRango('ratings',4,conn)
particionRoundRobin('ratings',4,conn)
conn.commit()
conn.close()
