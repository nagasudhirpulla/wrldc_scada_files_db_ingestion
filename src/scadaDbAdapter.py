import psycopg2
from src.appConf import getConf
import datetime as dt

class ScadaDbAdapter:
    conn = None

# object attributes = {'util_name', 'block', 'sch_type', 'val', 'sch_date'}
# schedules table columns = sch_utility, sch_date, sch_block, sch_type, sch_val
    def pushRows(self, dataRows):
        cur = self.conn.cursor()
        # we will commit in multiples of 150 rows
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

                dataInsertionTuple = (dt.datetime.strftime(dataRow['meas_time'], '%Y-%m-%d %H:%M:%S'), dataRow['meas_tag'],
                                      dataRow['meas_val'])
                dataInsertionTuples.append(dataInsertionTuple)

            # prepare sql for insertion and execute
            dataText = ','.join(cur.mogrify('(%s,%s,%s)', row).decode(
                "utf-8") for row in dataInsertionTuples)
            sqlTxt = 'INSERT INTO public.meas_time_data(\
        	meas_time, meas_tag, meas_val)\
        	VALUES {0} on conflict (meas_tag, meas_time) \
            do update set meas_val = excluded.meas_val'.format(dataText)
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
