import psycopg2
from appConf import getConf
import datetime as dt


class NodeStatusDbAdapter:
    conn = None

# object attributes = data_time,ip,name,status
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

                dataInsertionTuple = (dt.datetime.strftime(dataRow['data_time'], '%Y-%m-%d %H:%M:%S'), dataRow['name'],
                                      dataRow['ip'], dataRow['status'])
                dataInsertionTuples.append(dataInsertionTuple)

            # prepare sql for insertion and execute
            dataText = ','.join(cur.mogrify('(%s,%s,%s)', row).decode(
                "utf-8") for row in dataInsertionTuples)
            sqlTxt = 'INSERT INTO public.real_node_status(\
        	data_time, name, ip, status)\
        	VALUES {0} on conflict (ip) \
            do update set status = excluded.status, data_time=excluded.data_time'.format(dataText)
            cur.execute(sqlTxt)
            self.conn.commit()

            rowIter = iteratorEndVal

        # close cursor and connection
        cur.close()
        return True

    def connectToDb(self):
        dbConfig = getConf("dbConfig")
        self.conn = psycopg2.connect(host=dbConfig['db_host'], dbname=dbConfig['db_name'],
                                     user=dbConfig['db_username'], password=dbConfig['db_password'])

    def disconnectDb(self):
        self.conn.close()
