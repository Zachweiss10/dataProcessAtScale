#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2

def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    Lines = open(ratingsfilepath, 'r')
    #batch_size = len(Lines)
    drop_query = 'DROP TABLE IF EXISTS "%s";' % (ratingstablename)
    create_query = 'CREATE TABLE "%s"( \n' \
                   'UserID INTEGER, x1 VARCHAR(1), \n' \
                   'MovieID INTEGER, x2 VARCHAR(2), Rating REAL, x3 VARCHAR(3), \n' \
                   'Timestamp INTEGER); ' %(ratingstablename)
    conn = openconnection.cursor()
    conn.execute(drop_query)
    conn.execute(create_query)
    conn.copy_from(Lines, ratingstablename, sep=':',
                  columns=('UserID', 'x1', 'MovieID', 'x2', 'Rating', 'x3','Timestamp'))
    conn.execute("ALTER TABLE "+ratingstablename+" DROP COLUMN Timestamp, DROP COLUMN x1, DROP COLUMN x2, DROP COLUMN x3;")
    conn.close()
    return

def createtable(tablename, conn):
    create_table_query = 'CREATE TABLE "%s" ( \n' \
			 '"%s" INTEGER, "%s" INTEGER, "%s" REAL \n)' % (tablename, 'userid', 'movieid', 'rating')
    conn.execute(create_table_query)
 

def rangePartition(ratingstablename, numberofpartitions, openconnection):
    drop_query = 'DROP TABLE IF EXISTS range_part'
    #5.0 is the max rating so we need to find number of buckets for			  
    Bucket = 5.0/ numberofpartitions
    conn = openconnection.cursor()
    for i in range(0, numberofpartitions):
        tablename = 'range_part'+str(i)
	createtable(tablename, conn)
	low = Bucket * i
	high = low + Bucket
	if low == 0.0:
	    query = 'INSERT INTO "%s" \n' \
		    'SELECT * FROM "%s" \n' \
		    'WHERE rating >= %s \n' \
		    'AND rating <= %s;' %(tablename, ratingstablename, low, high)
	else: 
	     query = 'INSERT INTO "%s" \n' \
		    'SELECT * FROM "%s" \n' \
		    'WHERE rating > %s \n' \
		    'AND rating <= %s;' %(tablename, ratingstablename, low, high)
        conn.execute(query)
    conn.close()    
  


def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    modvalue = 0
    conn = openconnection.cursor()
    for i in range( 0, numberofpartitions ):
        tablename = 'rrobin_part' + str(i)
        createtable(tablename,conn)
	if (numberofpartitions == i + 1):
    		modnum = 0;
	else:
		modnum = i + 1;	
	
	
	query = 'INSERT INTO "%s" \n' \
	        'SELECT "userid","movieid","rating" \n' \
                'FROM (SELECT ROW_NUMBER() OVER() as index,* FROM "%s") AS push \n ' \
                'WHERE MOD(index,%s) = %s; ' % (tablename,ratingstablename, int(numberofpartitions), int(modnum))

	conn.execute(query) 

def numOfTables(conn):
    query = 'SELECT COUNT(table_name) \n' \
	    'FROM information_schema.tables;'
    conn.execute(query)
    count = int(conn.fetchone()[0])
    print(count)
    return count

def numOfRows(table, conn):
    query = 'SELECT COUNT(*) \n' \
	    'FROM "%s";' % (table)
    conn.execute(query)
    count = int(conn.fetchone()[0])
    return count

def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    conn = openconnection.cursor()
    count = numOfTables(conn)
    j = 0
    prevRowCount = numOfRows('rrobin_part'+str(0), conn)
    for i in range(1, prevRowCount):
	nextRowCount = numOfRows('rrobin_part'+str(i), conn)
	if prevRowCount > nextRowCount:
	    j = i
    query = 'INSERT INTO "%s" \n' \
	    'VALUES ( %s, %s, %s );' %(ratingstablename,int(userid), int(itemid), float(rating))
    conn.execute(query)
    query = 'INSERT INTO "%s" \n' \
	    'VALUES ( %s, %s, %s );' %('rrobin_part'+str(j), int(userid), int(itemid), float(rating))
    conn.execute(query)
    return
    

def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    conn = openconnection.cursor()
    count = numOfTables(conn)
    Bucket = 5.0/ count
    part = -1
    for i in range(0, count):
	part = i
    	low = Bucket * i; high = low + Bucket;
    print(userid, itemid, rating)
    if part >= 0:
        query = 'INSERT INTO "%s" \n' \
		'VALUES ( %s, %s, %s ) ;' %(ratingstablename, int(userid), int(itemid), float(rating))
	conn.execute(query)
	query = 'INSERT INTO "%s" \n' \
		'VALUES ( %s, %s, %s ) ;' %('range_part'+str(2), int(userid), int(itemid), float(rating))
	conn.execute(query)
    return
    
	    

def createDB(dbname='dds_assignment'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.close()

def deletepartitionsandexit(openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    l = []
    for row in cur:
        l.append(row[0])
    for tablename in l:
        cur.execute("drop table if exists {0} CASCADE".format(tablename))

    cur.close()

def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
    finally:
        if cursor:
            cursor.close()