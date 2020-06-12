import psycopg2
from appConf import getConf
import datetime as dt
import pandas as pd


class NodeStatusDbAdapter:
    conn = None

    # object attributes = data_time,ip,name,status,last_toggled_at
    # schedules table columns = data_time,ip,name,status
    def pushRows(self, dataRows):
        cur = self.conn.cursor()
        # we will commit in multiples of 500 rows
        rowIter = 0
        insIncr = 500
        numRows = len(dataRows)
        while rowIter < numRows:
            # set iteration values
            iteratorEndVal = rowIter+insIncr
            if iteratorEndVal >= numRows:
                iteratorEndVal = numRows

            # Create row tuples
            dataInsertionTuples = []
            for insRowIter in range(rowIter, iteratorEndVal):
                dataRow = dataRows[insRowIter]

                dataInsertionTuple = (dt.datetime.strftime(dataRow['data_time'], '%Y-%m-%d %H:%M:%S'),
                                      dt.datetime.strftime(
                                          dataRow['last_toggled_at'], '%Y-%m-%d %H:%M:%S'),
                                      dataRow['name'], dataRow['ip'], str(dataRow['status']))
                dataInsertionTuples.append(dataInsertionTuple)

            # prepare sql for insertion and execute
            dataText = ','.join(cur.mogrify('(%s,%s,%s,%s,%s)', row).decode(
                "utf-8") for row in dataInsertionTuples)
            sqlTxt = 'INSERT INTO public.real_node_status(\
        	data_time, last_toggled_at, name, ip, status)\
        	VALUES {0} on conflict (name) \
            do update set status = excluded.status, \
            last_toggled_at=excluded.last_toggled_at, \
            ip=excluded.ip, data_time=excluded.data_time'.format(dataText)
            cur.execute(sqlTxt)
            self.conn.commit()

            rowIter = iteratorEndVal

        # close cursor and connection
        cur.close()
        return True

    # object attributes = data_time,name,status
    # table columns = data_time,name,status
    def pushHistRows(self, dataRows):
        cur = self.conn.cursor()
        # we will commit in multiples of 500 rows
        rowIter = 0
        insIncr = 500
        numRows = len(dataRows)
        while rowIter < numRows:
            # set iteration values
            iteratorEndVal = rowIter+insIncr
            if iteratorEndVal >= numRows:
                iteratorEndVal = numRows

            # Create row tuples
            dataInsertionTuples = []
            for insRowIter in range(rowIter, iteratorEndVal):
                dataRow = dataRows[insRowIter]

                dataInsertionTuple = (dt.datetime.strftime(dataRow['data_time'], '%Y-%m-%d %H:%M:%S'), dataRow['name'],
                                      str(dataRow['status']))
                dataInsertionTuples.append(dataInsertionTuple)

            # prepare sql for insertion and execute
            dataText = ','.join(cur.mogrify('(%s,%s,%s)', row).decode(
                "utf-8") for row in dataInsertionTuples)
            sqlTxt = 'INSERT INTO public.node_status_history(\
        	data_time, name, status)\
        	VALUES {0} on conflict (name, data_time) \
            do update set status = excluded.status'.format(dataText)
            cur.execute(sqlTxt)
            self.conn.commit()

            rowIter = iteratorEndVal

        # close cursor and connection
        cur.close()
        return True

    def fetchLiveNodeStatus(self):
        cur = self.conn.cursor()
        try:
            sqlTxt = 'select name, status, data_time, last_toggled_at FROM public.real_node_status'
            cur.execute(sqlTxt)
            records = cur.fetchall()
            records = pd.DataFrame.from_records(
                records, columns=['name', 'status', 'data_time', 'last_toggled_at'])
        except (Exception, psycopg2.Error) as error:
            print("Error while fetching data from PostgreSQL", error)
            records = pd.DataFrame(
                columns=['name', 'status', 'data_time', 'last_toggled_at'])
        finally:
            cur.close()
            return records

    def fetchLatestNodeHistory(self):
        cur = self.conn.cursor()
        try:
            sqlTxt = 'select name, data_time, status FROM public.node_status_history\
                where (name, data_time) in (select name, max(data_time) \
                from public.node_status_history group by name)'
            cur.execute(sqlTxt)
            records = cur.fetchall()
            records = pd.DataFrame.from_records(records)
            records.columns = ['name', 'data_time', 'status']
        except (Exception, psycopg2.Error) as error:
            print("Error while fetching data from PostgreSQL", error)
            records = pd.DataFrame(columns=['name', 'data_time', 'status'])
        finally:
            cur.close()
            return records

    def connectToDb(self):
        dbConfig = getConf("dbConfig")
        self.conn = psycopg2.connect(host=dbConfig['db_host'], dbname=dbConfig['db_name'],
                                     user=dbConfig['db_username'], password=dbConfig['db_password'])

    def disconnectDb(self):
        self.conn.close()
