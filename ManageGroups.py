import flickrtodb as f
import flickrapi as fapi
import time


api_key = u'4a69280a31fc96c26e7d218c3a8cf345'
api_secret = u'a2d9a639fd955497'
myuserid = u'58051209@N00'

fo = f.FlickrToDb(myuserid, api_key, api_secret)




try:
    start = time.time()

    fo.init()

    grpid = fo.get_group("FlickrCentral")
    print(f'GroupID: {grpid}')
    if grpid != "":
        fo.clean_group(grpid, 23)

    print(f'Time: {time.time() - start}')
except fapi.FlickrError as err:
    print("Flickr error {}".format(err))
finally:
    fo.end()

