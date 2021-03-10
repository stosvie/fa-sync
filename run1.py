from urllib.parse import unquote_plus
import flickrtodb as f
import flickrapi as fapi
import time




def do_fasync():
    api_key = u'4a69280a31fc96c26e7d218c3a8cf345'
    api_secret = u'a2d9a639fd955497'
    myuserid = u'58051209@N00'
    retobj = {}

    fo = f.FlickrToDb(myuserid, api_key, api_secret)
    try:
        start = time.time()

        fo.init()

        # fo.get_totals_stats('2020-03-01')
        fo.get_stats_batch()
        # fo.get_user_photos()
        # fo._test_photos()
        # fo._test_single_photo(49576196772)
        retobj = {"status": "sucess", "loadtime" :time.time() - start}

        print(f'Time: {time.time() - start}')
        return retobj
        
    except fapi.FlickrError as err:
        print("Flickr error {}".format(err))
        retobj = {"status": "error", "errortxt" :err}
        return retobj

    finally:
        fo.end()

d = do_fasync()
print(d)
