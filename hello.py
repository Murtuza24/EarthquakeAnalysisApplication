import os
from flask import Flask,redirect,render_template,request
import urllib
import datetime
import json
import ibm_db
import pandas as pa
import dateutil.parser
from geopy.distance import great_circle
from datetime import datetime
from dateutil import tz

app = Flask(__name__)

# get service information if on IBM Cloud Platform
if 'VCAP_SERVICES' in os.environ:
    db2info = json.loads(os.environ['VCAP_SERVICES'])['dashDB For Transactions'][0]
    db2cred = db2info["credentials"]
    appenv = json.loads(os.environ['VCAP_APPLICATION'])

elif os.path.isfile('vcap-local.json'):
   with open('vcap-local.json') as f:
       db2cred = json.load(f)
       print('Found local VCAP_SERVICES')

else:
    raise ValueError('Expected cloud environment')

@app.route('/', methods =['GET','POST'])
def home(name=None):
    return render_template('earthquake.html')

# handle database request and query city information
@app.route('/search', methods =['GET','POST'])
def searchMagnitude(mag=None):
    # connect to DB2

    db2connection = ibm_db.connect("DATABASE="+db2cred['db']+";HOSTNAME="+db2cred['hostname']+";PORT="+str(db2cred['port'])+";UID="+db2cred['username']+";PWD="+db2cred['password']+";","","")
    if db2connection:
        # we have a Db2 connection, query the database

        mag = request.args.get('mag', '')

        sql="select * from earthquakes where \"mag\"= ?"

        # Note that for security reasons we are preparing the statement first,
        # then bind the form input as value to the statement to replace the
        # parameter marker.
        stmt = ibm_db.prepare(db2connection,sql)
        ibm_db.bind_param(stmt, 1, mag)
        ibm_db.execute(stmt)
        rows=[]
        # fetch the result
        result = ibm_db.fetch_assoc(stmt)
        while result != False:
            rows.append(result.copy())
            result = ibm_db.fetch_assoc(stmt)
            # print(result)
        # close database connection
        ibm_db.close(db2connection)

        rows = getLocalTime(rows)

    return render_template('earthquake.html', ci=rows)

@app.route('/searchStateWithMaxMag', methods =['GET','POST'])
def searchStateWithMaxMag(mag=None):
    # connect to DB2

    db2connection = ibm_db.connect("DATABASE="+db2cred['db']+";HOSTNAME="+db2cred['hostname']+";PORT="+str(db2cred['port'])+";UID="+db2cred['username']+";PWD="+db2cred['password']+";","","")
    if db2connection:
        # we have a Db2 connection, query the database

        lpolar = request.args.get('lpolar', '')
        hpolar = request.args.get('hpolar', '')

        sql="SELECT \"locationSource\", count(\"locationSource\") as LOCATIONCOUNT, MAX(\"mag\") as MAXMAG from earthquakes where \"locationSource\" IN " \
            "(select \"Location\" from polarity where \"Polarity\" between ? and ? ) group by \"locationSource\""

        # Note that for security reasons we are preparing the statement first,
        # then bind the form input as value to the statement to replace the
        # parameter marker.
        stmt = ibm_db.prepare(db2connection,sql)
        ibm_db.bind_param(stmt, 1, lpolar)
        ibm_db.bind_param(stmt, 2, hpolar)
        ibm_db.execute(stmt)
        data=[]
        # fetch the result
        result = ibm_db.fetch_assoc(stmt)
        while result != False:
            data.append(result.copy())
            result = ibm_db.fetch_assoc(stmt)
            # print(result)
        # close database connection
        ibm_db.close(db2connection)
        print(data)
        # rows = getLocalTime(rows)

    return render_template('polar_range_search.html', ci=data)

@app.route('/searchinrange', methods =['GET','POST'])
def searchMagInRange(lmag=None, hmag=10):
    # connect to DB2

    db2connection = ibm_db.connect("DATABASE="+db2cred['db']+";HOSTNAME="+db2cred['hostname']+";PORT="+str(db2cred['port'])+";UID="+db2cred['username']+";PWD="+db2cred['password']+";","","")
    if db2connection:
        # we have a Db2 connection, query the database

        lmag = request.args.get('lmag', '')
        hmag = request.args.get('hmag', '')
        net = request.args.get('net', '')
        attr = "\"mag\""
        sql="select * from earthquakes where \"mag\" between ? and ? and \"net\" = ?"

        # Note that for security reasons we are preparing the statement first,
        # then bind the form input as value to the statement to replace the
        # parameter marker.
        stmt = ibm_db.prepare(db2connection,sql)
        ibm_db.bind_param(stmt, 1, lmag)
        ibm_db.bind_param(stmt, 2, hmag)
        ibm_db.bind_param(stmt, 3, net)
        ibm_db.execute(stmt)
        rows=[]
        # fetch the result
        result = ibm_db.fetch_assoc(stmt)
        while result != False:
            rows.append(result.copy())
            result = ibm_db.fetch_assoc(stmt)
            # print(result)
        # close database connection
        ibm_db.close(db2connection)
    return render_template('earthquake.html', ci=rows)

@app.route('/searchindate', methods =['GET','POST'])
def searchDateRange(lmag=None, hmag=None):
    # connect to DB2

    db2connection = ibm_db.connect("DATABASE="+db2cred['db']+";HOSTNAME="+db2cred['hostname']+";PORT="+str(db2cred['port'])+";UID="+db2cred['username']+";PWD="+db2cred['password']+";","","")
    if db2connection:
        # we have a Db2 connection, query the database

        startdate = request.args.get('startdate', '')
        enddate = request.args.get('enddate', '')
        print(startdate)
        print(enddate)
        sql="select * from earthquakes where \"time\" between ? and ?"

        # Note that for security reasons we are preparing the statement first,
        # then bind the form input as value to the statement to replace the
        # parameter marker.
        stmt = ibm_db.prepare(db2connection,sql)
        ibm_db.bind_param(stmt, 1, startdate)
        ibm_db.bind_param(stmt, 2, enddate)
        ibm_db.execute(stmt)
        rows=[]
        # fetch the result
        result = ibm_db.fetch_assoc(stmt)
        while result != False:
            rows.append(result.copy())
            result = ibm_db.fetch_assoc(stmt)
            # print(result)
        # close database connection
        ibm_db.close(db2connection)
    return render_template('earthquake.html', ci=rows)

@app.route('/searchDayNight', methods =['GET','POST'])
def searchDayNight(mag=None, starttime=None, endtime = None):
    # connect to DB2

    db2connection = ibm_db.connect("DATABASE="+db2cred['db']+";HOSTNAME="+db2cred['hostname']+";PORT="+str(db2cred['port'])+";UID="+db2cred['username']+";PWD="+db2cred['password']+";","","")
    if db2connection:
        # we have a Db2 connection, query the database

        mag = request.args.get('mag', '')
        # starttime = request.args.get('starttime', '')
        # starttime = starttime[:2]
        # endtime = request.args.get('endtime', '')
        # endtime = endtime[:2]
        sql = "select count(*) from earthquakes where \"mag\" > ? and (EXTRACT(hour FROM \"time\") >= 6 OR EXTRACT(hour FROM \"time\") <= 21)"

        stmt = ibm_db.prepare(db2connection,sql)
        ibm_db.bind_param(stmt, 1, mag)
        # ibm_db.bind_param(stmt, 2, starttime)
        # ibm_db.bind_param(stmt, 3, endtime)
        ibm_db.execute(stmt)
        print(sql)
        day=[]
        # fetch the result
        result = ibm_db.fetch_assoc(stmt)
        while result != False:
            day.append(result.copy())
            result = ibm_db.fetch_assoc(stmt)

        # for night count
        sql2 = "select count(*) from earthquakes where \"mag\" > ? and (EXTRACT(hour FROM \"time\") >= 21 OR EXTRACT(hour FROM \"time\") <= 6)"

        stmt = ibm_db.prepare(db2connection, sql2)
        ibm_db.bind_param(stmt, 1, mag)
        # ibm_db.bind_param(stmt, 2, starttime)
        # ibm_db.bind_param(stmt, 3, endtime)
        ibm_db.execute(stmt)
        night = []
        # fetch the result
        result = ibm_db.fetch_assoc(stmt)
        while result != False:
            night.append(result.copy())
            result = ibm_db.fetch_assoc(stmt)

        ibm_db.close(db2connection)

    return render_template('location.html', ci=day, ci2=night)

@app.route('/searchBoxRange', methods =['GET','POST'])
def searchBoxRange(lat1=None, long1=None, lat2=None, long2 = None):
    # connect to DB2

    db2connection = ibm_db.connect("DATABASE="+db2cred['db']+";HOSTNAME="+db2cred['hostname']+";PORT="+str(db2cred['port'])+";UID="+db2cred['username']+";PWD="+db2cred['password']+";","","")
    if db2connection:
        # we have a Db2 connection, query the database

        lat1 = request.args.get('lat1', '')
        long1 = request.args.get('long1', '')
        lat2 = request.args.get('lat2', '')
        long2 = request.args.get('long2', '')
        print(lat1)
        print(long1)
        print(lat2)
        print(long2)
        sql="select * from earthquakes where \"latitude\" between ? and ? and \"longitude\" between ? and ?"


        # Note that for security reasons we are preparing the statement first,
        # then bind the form input as value to the statement to replace the
        # parameter marker.
        stmt = ibm_db.prepare(db2connection,sql)
        ibm_db.bind_param(stmt, 1, lat1)
        ibm_db.bind_param(stmt, 2, long1)
        ibm_db.bind_param(stmt, 3, lat2)
        ibm_db.bind_param(stmt, 4, long2)
        ibm_db.execute(stmt)
        print(sql)
        rows=[]
        # fetch the result
        result = ibm_db.fetch_assoc(stmt)
        while result != False:
            rows.append(result.copy())
            result = ibm_db.fetch_assoc(stmt)
            # print(result)
        # close database connection
        ibm_db.close(db2connection)
        print(len(rows))
    return render_template('earthquake.html', ci=rows)


@app.route("/searchByRad")
def findRadius():
    db2conn = ibm_db.connect(
        "DATABASE=" + db2cred['db'] + ";HOSTNAME=" + db2cred['hostname'] + ";PORT=" + str(db2cred['port']) + ";UID=" +
        db2cred['username'] + ";PWD=" + db2cred['password'] + ";", "", "")

    if db2conn:
        radius = request.args.get('radius', '')
        latitude = request.args.get('latitude', '')
        longitude = request.args.get('longitude', '')

        # sql = 'select * from earthquake'
        # sql = 'select * from (SELECT , (((acos(sin(( ? *3.14/180)) sin(("latitude"*3.14/180)) + cos(( ? * 3.14/180 )) * cos(("latitude" 3.14/180)) * cos((( ? - "longitude")*3.14/180  )))) *180/3.14)*60 1.1515*1.609344) as distance FROM earthquakes) WHERE distance <= ? '
        sql = 'select * from (SELECT , (((acos(sin(( ? *3.14/180)) sin(("latitude" * 3.14 / 180)) + cos(( ? * 3.14/180 )) * cos(("latitude"* 3.14/180)) * cos((( ? - "longitude")3.14/180  )))) *180/3.14)*60 1.1515*1.609344) as distance FROM earthquake) WHERE distance <= ?'
        print(sql)

        stmt = ibm_db.prepare(db2conn, sql)
        ibm_db.bind_param(stmt, 1, latitude)
        ibm_db.bind_param(stmt, 2, latitude)
        ibm_db.bind_param(stmt, 3, longitude)
        ibm_db.bind_param(stmt, 4, radius)
        ibm_db.execute(stmt)
        rows = []

        result = ibm_db.fetch_assoc(stmt)  # fetch the result
        while result != False:
            rows.append(result.copy())
            result = ibm_db.fetch_assoc(stmt)
            print(result)
        ibm_db.close(db2conn)  # close database connection

    '''
    cordinate1 = (latitude, longitude)
    newList = []

    for i,num in enumerate(rows):
        cordinate2 = (num["latitude"], num["longitude"])
        distance = great_circle(cordinate1, cordinate2).kilometers
        # print(great_circle(newport_ri, cleveland_oh).miles)
        if (float(distance) <= float(radius)):
            newList.append(rows[i])
    '''
    return render_template("home.html", data=rows)

@app.route('/searchByRadius', methods =['GET','POST'])
def searchByRadius(radius=None, latitude=None, longitude = None):
    # connect to DB2

    db2connection = ibm_db.connect("DATABASE="+db2cred['db']+";HOSTNAME="+db2cred['hostname']+";PORT="+str(db2cred['port'])+";UID="+db2cred['username']+";PWD="+db2cred['password']+";","","")
    if db2connection:
        # we have a Db2 connection, query the database

        radius = request.args.get('radius', '')
        latitude = request.args.get('latitude', '')
        longitude = request.args.get('longitude', '')
        mag = request.args.get('mag', '')
        print(radius)
        print(latitude)
        print(longitude)
        print(mag)
        sql="select * from (SELECT *, (((acos(sin(( ? *3.14/180))* sin((\"latitude\"*3.14/180)) " \
            "+ cos(( ? * 3.14/180 )) * cos((\"latitude\" *3.14/180)) * cos((( ? - \"longitude\")" \
            "*3.14/180  )))) *180/3.14)*60* 1.1515*1.609344) as distance FROM earthquakes)" \
            " WHERE distance <= ? and \"mag\"> ?"
        print(sql)

        stmt = ibm_db.prepare(db2connection,sql)
        ibm_db.bind_param(stmt, 1, latitude)
        ibm_db.bind_param(stmt, 2, latitude)
        ibm_db.bind_param(stmt, 3, longitude)
        ibm_db.bind_param(stmt, 4, radius)
        ibm_db.bind_param(stmt, 5, mag)
        ibm_db.execute(stmt)
        print(sql)
        rows=[]
        newList = []
        # fetch the result
        result = ibm_db.fetch_assoc(stmt)
        while result != False:
            rows.append(result.copy())
            result = ibm_db.fetch_assoc(stmt)
            # print(result)
        # close database connection
        ibm_db.close(db2connection)
        n = (len(rows))
        print(rows)
        print(n)
        # cordinate1 = (latitude, longitude)
        #
        # for i,num in enumerate(rows):
        #     cordinate2 = (num["latitude"], num["longitude"])
        #     distance = great_circle(cordinate1, cordinate2).miles
        #     # print(great_circle(newport_ri, cleveland_oh).miles)
        #     if (float(distance) <= float(radius)):
        #         newList.append(rows[i])


    return render_template('earthquake.html', ci=newList)

@app.route('/visualizeData', methods = ['GET','POST'])
def visualizeData():
    return render_template('earthquake.html')

def getLocalTime(rows):
    from_zone = tz.gettz('UTC')
    to_zone = tz.gettz('America/Chicago')

    for i in rows:

        utc = datetime.strptime(i['time'].strftime('%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S')
        utc = utc.replace(tzinfo=from_zone)
        central = utc.astimezone(to_zone)
        i['time'] = central
    return rows


@app.route('/earthquake_in_intervals', methods=['GET', 'POST'])
def earthquake_in_intervals():
            magnitude_low = request.args.get('magnitude_low', '2.0', type=str)
            magnitude_high = request.args.get('magnitude_high', '2.5', type=str)
            magnitude_low_float = float(magnitude_low)
            magnitude_high_float = float(magnitude_high)
            # connect to DB

            db2connection = ibm_db.connect(
                "DATABASE=" + db2cred['db'] + ";HOSTNAME=" + db2cred['hostname'] + ";PORT=" + str(
                    db2cred['port']) + ";UID=" + db2cred['username'] + ";PWD=" + db2cred['password'] + ";", "", "")
            if db2connection:

                while (magnitude_low_float != magnitude_high_float):
                    interval = magnitude_low + " to " + magnitude_high
                    sql = "select count(*) from earthquake where \"mag\" BETWEEN " +str(magnitude_low)+" and " + str(magnitude_high)
                    stmt = ibm_db.prepare(db2connection, sql)

                    ibm_db.execute(stmt)
                    rows = []
                    data = ibm_db.fetch_assoc(stmt)

                    for item in data:
                        rows = str(item[0])
                    sql2 = "insert into tempdata (range, count) values (\'" + interval + "\', " + rows + ")"
                    return sql2
                    # cursor.execute(query)
                    stmt = ibm_db.prepare(db2connection, sql2)

                    ibm_db.execute(stmt)
                    magnitude_lowInt = magnitude_lowInt + 0.1

                sql3 = "select * from tempdata"
                stmt = ibm_db.prepare(db2connection, sql3)

                ibm_db.execute(stmt)
                data=[]
                result = ibm_db.fetch_assoc(stmt)
                while result != False:
                    data.append(result.copy())
                    result = ibm_db.fetch_assoc(stmt)
                    # print(result)
                # close database connection
                ibm_db.close(db2connection)
            return render_template('earthquake_in_intervals.html', data=data)


@app.route('/searchBasedOnDepth', methods=['GET', 'POST'])
def searchBasedOnDepth():

    db2connection = ibm_db.connect(
        "DATABASE=" + db2cred['db'] + ";HOSTNAME=" + db2cred['hostname'] + ";PORT=" + str(db2cred['port']) + ";UID=" +
        db2cred['username'] + ";PWD=" + db2cred['password'] + ";", "", "")
    if db2connection:
        depth_low = int(request.args.get('depth_low', ''))
        depth_high = int(request.args.get('depth_high', ''))
        nst = int(request.args.get('nst', ''))
        step = 10
        for i in range(depth_low, depth_high, step):
        # while(depth_low < depth_high):
            sql = "select count(*) from earthquakes where \"depth\" between ? and ? and \"nst\" > ?"
            # Note that for security reasons we are preparing the statement first,
            # then bind the form input as value to the statement to replace the
            # parameter marker.
            stmt = ibm_db.prepare(db2connection, sql)
            ibm_db.bind_param(stmt, 1, i)
            ibm_db.bind_param(stmt, 2, i+10)
            ibm_db.bind_param(stmt, 3, nst)
            ibm_db.execute(stmt)
            rows = []
            # fetch the result
            result = ibm_db.fetch_assoc(stmt)
            while result != False:
                rows.append(result.copy())
                result = ibm_db.fetch_assoc(stmt)

            print(str(i)+" "+str(i+10)+" "+str(rows[0]))
            # depth_low += 10
            # print(result)
        # close database connection
        ibm_db.close(db2connection)
        # print(len(rows))

    return render_template('earthquake_in_intervals.html', data=rows)


@app.route("/magnitude_range_search")
def searchMag():
    db2conn = ibm_db.connect(
        "DATABASE=" + db2cred['db'] + ";HOSTNAME=" + db2cred['hostname'] + ";PORT=" + str(db2cred['port']) + ";UID=" +
        db2cred['username'] + ";PWD=" + db2cred['password'] + ";", "", "")

    if db2conn:
        magnitude1 = float(request.args.get('lmag', ''))
        magnitude2 = float(request.args.get('hmag', ''))
        net = request.args.get('net', '')

        print(magnitude1, magnitude2, net)

        m1 = magnitude1
        m2 = m1 + 0.5
        rows = []
        countMap = {}
        while m1 < magnitude2:
            sql = "select count(*) from earthquakes where \"mag\" between ? and ? and \"net\" = ?"
            stmt = ibm_db.prepare(db2conn, sql)
            ibm_db.bind_param(stmt, 1, m1)
            ibm_db.bind_param(stmt, 2, m2)
            ibm_db.bind_param(stmt, 3, net)
            ibm_db.execute(stmt)

            # fetch result
            result = ibm_db.fetch_assoc(stmt)
            while result != False:
                rows.append(result.copy())
                countMap[str(m1) + '-' + str(m2)] = result.copy()['1']
                result = ibm_db.fetch_assoc(stmt)
            m2 = m2 + 0.5
            m1 = m2 - .49

            print('rows:', str(rows[0]))
        print(countMap)
        # close database connection
        ibm_db.close(db2conn)  # close database connection

    return render_template("magnitude_range_search.html", data=countMap)



port = os.getenv('PORT', '5000')
if __name__ == "__main__":
    app.run(host='0.0.0.0', port=port)