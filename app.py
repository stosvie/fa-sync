import os

from flask import Flask,request,jsonify, make_response

from urllib.parse import unquote_plus
import json

import re
import requests
import time

import flickrtodb as f
import flickrapi as fapi

app = Flask(__name__)


def do_getallphotos():
    api_key = u'4a69280a31fc96c26e7d218c3a8cf345'
    api_secret = u'a2d9a639fd955497'
    myuserid = u'58051209@N00'
    retobj = {}

    fo = f.FlickrToDb(myuserid, api_key, api_secret)
    try:
        start = time.time()

        fo.init()
        fo.get_user_photos_quick()

    except fapi.FlickrError as err:
        print("Flickr error {}".format(err))
        retobj = {"status": "error", "errortxt" :err}
        return retobj

    finally:
        fo.end()


def do_manageflickrgroup():
    api_key = u'4a69280a31fc96c26e7d218c3a8cf345'
    api_secret = u'a2d9a639fd955497'
    myuserid = u'58051209@N00'
    retobj = {}

    fo = f.FlickrToDb(myuserid, api_key, api_secret)

    try:
        start = time.time()
        fo.init()

        grpid = fo.get_group("FlickrCentral")
        
        print(f'GroupID: {grpid}')
        if grpid != "":
            fo.clean_group(grpid, 23)

            print(f'Time: {time.time() - start}')
            retobj = {  "status": "sucess", 
                        "grpname" : "FlickrCentral", 
                        "grpID" : grpid,
                        "picsRemaining" :  23,
                        "loadtime" :time.time() - start
                    }
            return retobj


    except fapi.FlickrError as err:
        print("Flickr error {}".format(err))
        retobj = {"status": "error", "errortxt" :err}
        return retobj

    finally:
        fo.end()


def do_fasync():
    api_key = u'4a69280a31fc96c26e7d218c3a8cf345'
    api_secret = u'a2d9a639fd955497'
    myuserid = u'58051209@N00'
    retobj = {}

    fo = f.FlickrToDb(myuserid, api_key, api_secret)
    try:
        start = time.time()
        fo.init()

        fo.get_stats_batch()

        retobj = {"status": "sucess", "loadtime" :time.time() - start}

        print(f'Time: {time.time() - start}')
        return retobj
        
    except fapi.FlickrError as err:
        print("Flickr error {}".format(err))
        retobj = {"status": "error", "errortxt" :err}
        return retobj

    finally:
        fo.end()

def parse_request(req):
    """
    Parses application/json request body data into a Python dictionary
    """
    payload = req.get_data()
    payload = unquote_plus(payload)
    payload = re.sub('payload=', '', payload)
    payload = json.loads(payload)

    return payload

@app.route('/')
def hello():
    return 'Hello Flask from alpine-linux!'

@app.route('/api/fstat/cleanflickrgroup',methods=['POST'])
def cleanflickrgroup():
    if request.is_json:
        payload = request.get_json(silent=True)
        
        d = do_manageflickrgroup()

        response = requests.post(payload["callbackURL"],data=d)
        response = "all-ok"
        return (response, 200, None)
    
    resp = 'Execute daily stats!'
    return (resp, 200, None)

@app.route('/api/fstat/dailystats', methods=['POST'])
def getStatsDaily():
    if request.is_json:
        payload = request.get_json(silent=True)
        
        d = do_fasync()

        response = requests.post(payload["callbackURL"],data=d)
        response = "all-ok"
        return (response, 200, None)
    
    
    resp = 'Execute daily stats!'
    return (resp, 200, None)
    #return 'Execute daily stats!'

@app.route('/api/fstat/getphotos', methods=['POST'])
def getAllPhotos():    
        
    d = do_getallphotos()

    response = "all-ok"
    return (response, 200, None)


if __name__ == '__main__':
    # Bind to PORT if defined, otherwise default to 5000.
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
