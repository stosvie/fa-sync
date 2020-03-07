import datetime
from datetime import date, timedelta
import time
import timeit

from urllib import parse

import flickrapi

import pandas as pd
# import numpy as np
import sqlalchemy as sa
# from sqlalchemy.orm import sessionmaker
# from sqlalchemy.sql import text
import webbrowser
from dateutil import parser


class db:
    engine = None
    connection = None
    schema = 'fs'

    def connect(self, server, dbname, username, pwd):

        connecting_string = 'DRIVER={ODBC Driver 17 for SQL Server};Server=%s;Database=%s;UID=%s;PWD=%s;TDS_Version=8.0;Port=1433;'
        connecting_string = connecting_string % (server, dbname, username, pwd)
        params = parse.quote_plus(connecting_string)

        self.engine = sa.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params, fast_executemany=True)
        self.connection = self.engine.connect()

    def write_df(self, dt, df, userid):

        ### TODO rewrite so that close date is called only once and trasaction goes over the
        ### whole list of dataframes to write
        if df.shape[0] > 0:

            try:
                start = time.time()
                trans = self.connection.begin()
                ### TODO replace with SP
                query = """
                        IF OBJECT_ID('fs.{}') IS NOT NULL 
                        delete from fs.{} where statdate = CAST( ? AS DATE) AND userid = ?; 
                        """.format(df.name, df.name)

                params = (dt, userid)
                #print(query)

                ### TODO schema as class property
                res = self.connection.execute(query, params, schema=self.schema)
                print(f'Delete statements deleted {res.rowcount} rows')
                print(f'Dataframe {df.name} with {df.shape[0]} rows')
                df.to_sql(df.name, con=self.engine, if_exists='append', chunksize=1000, schema=self.schema)

                new_state = lambda x: 'live' if (dt == date.today()) else 'frozen'

                #query = "insert into {}.stats_status (stats_date,stats_state)\
                #    VALUES ('{}','{}')".format(self.schema, dt, new_state(dt))
                query = f"exec [{self.schema}].[CloseStatLoadForDate] @dt='{dt}'"

                self.connection.execute(query)
                print(f'Updated status for date {dt} with \'{new_state(dt)}\'')

                trans.commit()

            except:
                trans.rollback()
                raise
            finally:
                print(f'Time writing dataframe: {df.name}, {time.time() - start}')

    def terminate(self):
        self.connection.close()


class FlickrToDb:
    _userid = u''
    _apikey = u''
    _secret = u''
    _flickr = None
    sqldb = db()
    _photos_per_batch = 100

    def __init__(self, userid, apikey, secret ):
        self._userid = userid
        self._apikey = apikey
        self._secret = secret
        self._flickr = None
        pass

    def init(self):
        self.flickr_authenticate()
        self.sqldb.connect('tcp:woo.database.windows.net,1433', 'BYWS', 'boss', 's7#3QzOsB$J*^v3')

    def end(self):
        self.sqldb.terminate()

    def flickr_authenticate(self):
        self._flickr = flickrapi.FlickrAPI(self._apikey, self._secret, format='parsed-json')
        print('Step 1: authenticate')

        # Only do this if we don't have a valid token already
        if not self._flickr.token_valid(perms='read'):
            # Get a request token
            self._flickr.get_request_token(oauth_callback='oob')

            # Open a browser at the authentication URL. Do this however
            # you want, as long as the user visits that URL.
            authorize_url = self._flickr.auth_url(perms='read')
            print(authorize_url)
            webbrowser.open_new_tab(authorize_url)

            # Get the verifier code from the user. Do this however you
            # want, as long as the user gives the application the code.
            verifier = str(input('Verifier code: '))

            # Trade the request token for an access token
            self._flickr.get_access_token(verifier)

    def get_dates(self):

        results = ()
        try:
            cursor = self.sqldb.connection.execute("EXEC fs.GetDatesToLoad")
            # fetch result parameters
            results = list(cursor.fetchall())
            cursor.close()
        except:
            raise
        finally:
            return results

    def _add_common_cols(self, df, dt ):
        ### TODO replace with insert, a one-liner
        dt_list = [dt for i in range(df.index.size)]
        user_list = [self._userid for i in range(df.index.size)]
        df['statdate'] = dt_list
        df['statdate'] = pd.to_datetime(df['statdate'])
        df['userid'] = user_list
        return df

    def _parse_col_tree(self, colid, df_cols):
        col_root = self._flickr.collections.getTree(user_id=self._userid, collection_id=colid)
        df_cols = df_cols.append(pd.DataFrame(col_root['collections']['collection']))

        for col in col_root['collections']['collection']:
            if 'collection' in col:
                for cn in col['collection']:
                    df_cols = self._parse_col_tree(cn['id'], df_cols)

        return df_cols

    def _get_photo_domains(self, df, dom_func, referrers_func,dt):

        df_domains = pd.DataFrame()
        for ph in df:
            popular = dom_func(date=dt, per_page=100, page=1, photo_id=ph)

            if int(popular['domains']['pages']) > 0:

                print("""Photo with id {} \
                                reports total of {} pages \
                                and has a list of {} domains,\
                                total is {}""".format(ph,
                                                    popular['domains']['pages'],
                                                    len(popular['domains']['domain']),
                                                    popular['domains']['total']))
                df_domains = df_domains.append(pd.DataFrame(popular['domains']['domain']))
                dt_domain = [dt for i in range(df_domains.index.size)]
                df_domains['statdate'] = dt_domain
                ph_domain = [ph for i in range(df_domains.index.size)]
                df_domains['photoid'] = ph_domain
                for dom in df_domains['name']:
                    refs = referrers_func(date=dt, photo_id = ph, domain=dom, per_page=100, page=1)
                    df_refs = pd.DataFrame(refs['domain']['referrer'])

                while popular['domains']['pages'] - popular['domains']['page'] > 0:
                    popular = dom_func(date=dt, per_page=100, page=popular['domains']['page']+1, photo_id=ph)
                    df_domains = df_domains.append(pd.DataFrame(popular['domains']['domain']))
                    dt_domain = [dt for i in range(df_domains.index.size)]
                    df_domains['statdate'] = dt_domain
                    ph_domain = [ph for i in range(df_domains.index.size)]
                    df_domains['photoid'] = ph_domain

        return df_domains

    def _get_domains(self, dom_func, referrers_func, d):
        res = self._domains_helper(dom_func, referrers_func, d, 1)
        final_df = res[0]

        while res[1] - res[2] > 0:
            res = self._domains_helper(dom_func, referrers_func, d, res[2] + 1)
            final_df = final_df.append(res[0])

        dt_list = [d for i in range(final_df.index.size)]
        final_df['statdate'] = dt_list
        return final_df

    def _domains_helper(self, dom_func, referrers_func, dt, pg):
        popular = dom_func(date=dt, per_page=100, page=pg)
        final_outer = pd.DataFrame()
        if int(popular['domains']['pages']) > 0:
            df_domains = pd.DataFrame(popular['domains']['domain'])

            for dom in popular['domains']['domain']:
                refs = referrers_func(date=dt, domain=dom['name'], per_page=100, page=1)
                df_refs = pd.DataFrame(refs['domain']['referrer'])
                dt_domain = [dom['name'] for i in range(df_refs.index.size)]
                df_refs['domain'] = dt_domain
                final_df = df_refs
                while refs['domain']['pages'] - refs['domain']['page'] > 0:
                    refs = referrers_func(date=dt, domain=dom['name'], per_page=100, page=refs['domain']['page'] + 1)
                    df_refs = pd.DataFrame(refs['domain']['referrer'])
                    dt_domain = [dom['name'] for i in range(df_refs.index.size)]
                    df_refs['domain'] = dt_domain
                    final_df = final_df.append(df_refs)

                final_outer = final_outer.append(final_df)

        if not 'searchterm' in final_outer.columns:
            searchterm = [None for i in range(final_outer.index.size)]
            final_outer['searchterm'] = searchterm

        return final_outer, popular['domains']['pages'], popular['domains']['page']

    def get_photo_stats(self, dt):

        print(f"Retrieving photo stats {dt} (user:{self._userid})")
        popular = self._flickr.stats.getPopularPhotos(date=dt, per_page=100, page=0)
        df_popular = pd.DataFrame(popular['photos']['photo'])

        while popular['photos']['pages'] - popular['photos']['page'] > 0:
            popular = self._flickr.stats.getPopularPhotos(date=dt, per_page=100, page=popular['photos']['page']+1)
            df_popular = df_popular.append(pd.DataFrame(popular['photos']['photo']))

        df_popular = df_popular[['id', 'title', 'stats']]
        df_stats = pd.DataFrame(df_popular['stats'].values.tolist())
        df_popular.reset_index(inplace=True, drop=True)
        df_popular = df_popular.join(df_stats)
        df_popular.drop('stats', axis=1, inplace=True)

        dt_list = [dt for i in range(df_popular.index.size)]
        df_popular['statdate'] = dt_list

        photo_domain_stats = self._get_domains(self._flickr.stats.getPhotoDomains,
                                               self._flickr.stats.getPhotoReferrers, dt)
        t = df_popular['id']
        #retval = self._get_photo_domains(t, self._flickr.stats.getPhotoDomains,
        #                                         self._flickr.stats.getPhotoReferrers, dt)

        #print(df_popular)

        ### TODO table names should be configurable!
        df_popular.name = 'stats_photos'
        photo_domain_stats.name = 'stats_photos_domains'

        return (self._add_common_cols(df_popular, dt), self._add_common_cols(photo_domain_stats, dt) )

    def get_totals_stats(self, dt):
        print(f"Retrieving totals stats {dt} (user:{self._userid})")
        totals = self._flickr.stats.getTotalViews(date=dt)
        df_totals = pd.DataFrame([{'date': dt,
                                   'total': totals['stats']['total']['views'],
                                   'photos': totals['stats']['photos']['views'],
                                   'photostream': totals['stats']['photostream']['views'],
                                   'sets': totals['stats']['sets']['views'],
                                   'galleries': totals['stats']['galleries']['views'],
                                   'collections': totals['stats']['collections']['views']
                                   }])
        df_totals.name = 'stats_totals'
        # all time

        totals = self._flickr.stats.getTotalViews()
        df_totals_alltime = pd.DataFrame([{'date': dt,
                                   'total': totals['stats']['total']['views'],
                                   'photos': totals['stats']['photos']['views'],
                                   'photostream': totals['stats']['photostream']['views'],
                                   'sets': totals['stats']['sets']['views'],
                                   'galleries': totals['stats']['galleries']['views'],
                                   'collections': totals['stats']['collections']['views']
                                   }])

        #print(df_totals)
        df_totals_alltime.name = 'stats_totals_alltime'
        return self._add_common_cols(df_totals, dt), self._add_common_cols(df_totals_alltime, dt)

    def get_set_stats(self, dt):

        print(f"Retrieving set stats {dt} (user:{self._userid})")
        photosets = self._flickr.photosets.getList(user_id=self._userid, per_page=100, page=1)
        df_sets = pd.DataFrame(photosets['photosets']['photoset'])
        while photosets['photosets']['pages'] - photosets['photosets']['page'] > 0:
            photosets = self._flickr.people.photosetsGetList(user_id=self._userid, per_page=100,
                                                        page=photosets['photossets']['page'] + 1)
            df_sets = df_sets.append(pd.DataFrame(photosets['photosets']['photoset']))

        ## drop unecessary columns
        df_sets = df_sets.drop(['secret', 'server', 'farm', 'primary'], 1)

        ## copy out description and title to normalize
        titles = [i['_content'] for i in df_sets['title'].values]
        descriptions = [i['_content'] for i in df_sets['description'].values]
        df_sets = df_sets.drop(['title', 'description'], 1)
        df_sets['title'] = pd.Series(titles)
        df_sets['description'] = pd.Series(descriptions)

        df_setstats = pd.DataFrame()
        for setid in df_sets['id']:
            ss = self._flickr.stats.getPhotosetStats(photoset_id=setid, date=dt)
            df_setstats = df_setstats.append(
                pd.DataFrame([{'date': dt, 'views': ss['stats']['views'], 'comments': ss['stats']['comments']}]))

        sets_domain_stats = self._get_domains(self._flickr.stats.getPhotosetDomains,
                                              self._flickr.stats.getPhotosetReferrers,dt)
        df_sets = df_sets.join(df_setstats.reset_index(drop=True))
        #print(df_setstats)
        df_setstats.name = 'stats_sets'
        sets_domain_stats.name = 'stats_sets_domains'
        return self._add_common_cols(df_setstats, dt), self._add_common_cols(sets_domain_stats, dt)

    def get_collection_stats(self, dt):
        
        print(f"Retrieving collections stats {dt} (user:{self._userid})")
        df_cols = pd.DataFrame()
        df_cols = self._parse_col_tree(0, df_cols)

        ## drop unecessary columns
        df_cols = df_cols[['id', 'title']]
        df_colstats = pd.DataFrame()

        for colid in df_cols['id']:
            try:
                realid = colid[colid.find('-')+1:]
                cs = self._flickr.stats.getCollectionStats(collection_id=realid, date=dt)
            except flickrapi.exceptions.FlickrError:
                print(flickrapi.exceptions.FlickrError)
            df_colstats = df_colstats.append(pd.DataFrame([{'date': dt, 'id': colid, 'views': cs['stats']['views'],
                                                            'title': df_cols.loc[df_cols['id'] == colid]['title'][0]}]))

        cols_domain_stats = self._get_domains(self._flickr.stats.getCollectionDomains,
                                              self._flickr.stats.getCollectionReferrers, dt)
        #print(df_colstats)
        df_colstats.name = 'stats_collections'
        cols_domain_stats.name = 'stats_collections_domains'
        return self._add_common_cols(df_colstats, dt), self._add_common_cols(cols_domain_stats, dt)

    def get_stream_stats(self, dt):

        print(f"Retrieving stream stats {dt} (user:{self._userid})")
        stream = self._flickr.stats.getPhotostreamStats(date=dt)
        df_streams = pd.DataFrame([{'date': dt, 'views': stream['stats']['views']}])

        stream_domain_stats = self._get_domains(self._flickr.stats.getPhotostreamDomains,
                                                self._flickr.stats.getPhotostreamReferrers, dt)
        #print(stream_domain_stats)
        df_streams.name = 'stats_streams'
        stream_domain_stats.name = 'stats_stream_domains'
        return self._add_common_cols(df_streams, dt), self._add_common_cols(stream_domain_stats, dt)

    def get_all_stats(self, dt):

        writelst = []
        print(f"Retrieving stats for date {dt} (user:{self._userid})")
        ### TODO need a decorator to get fine-grained timing
        start = time.time()
        
        writelst.extend(self.get_photo_stats(dt))
        writelst.extend(self.get_totals_stats(dt))
        writelst.extend(self.get_stream_stats(dt))
        writelst.extend(self.get_set_stats(dt))
        writelst.extend(self.get_collection_stats(dt))
        print(f'Total time for collecting flickr stats: {time.time() - start}')

        for i in writelst:
            self.sqldb.write_df(dt, i, self._userid)

        #print('retrieved all stats')
    def get_stats_batch(self):


        #print(ls)
        for dt in self.get_dates():
            self.get_all_stats(dt[0])
        #ps = self.get_photo_stats(dt[0])

    ### TODO this needs to be sorted
    #        1) add groups and more 
    #        2) all sub details need to recurs for all pages, unlikely but possible
    #        3) update should get photo details and list only those that have 
    #           a newer last updated date
    def _get_photo_batch(self, page_to_get):

        start = time.time()
        
        photos = self._flickr.people.getPhotos(user_id=self._userid, page=page_to_get)
        
        cur_page = photos['photos']['page']
        tot_pages = photos['photos']['pages']

        df2 = pd.DataFrame(photos['photos']['photo'])

        # calldb(df2,'photo_details')
        print(f'photo details has  {df2.shape[0]} rows.')
        
        df_allfaves = pd.DataFrame()
        df_alltags = pd.DataFrame()
        for f in photos['photos']['photo']:
                photoid = f['id']
                phototitle = f['title']

                faves =  self._flickr.photos.getFavorites(photo_id=f['id'], per_page=50)

                df_faves = pd.DataFrame(faves['photo']['person'])
                id_list = [photoid for i in range(df_faves.index.size)]
                df_faves['photo_id'] = id_list
                df_allfaves = df_allfaves.append(df_faves)
                
                # calldb(df_faves, 'photo_faves')
                #countfaves = faves['photo']['total']

                i1 = self._flickr.photos.getInfo(photo_id=photoid, format='parsed-json')
                df_tags = pd.DataFrame(i1['photo']['tags']['tag'])
                #photoids = ['photoid'] * range(df_tags.index.size)
                photoids = [photoid for i in range(df_tags.index.size)]
                df_tags['photo_id'] = photoids
                df_alltags = df_alltags.append(df_tags)
                #calldb(df_tags, 'photo_tags')
        
        print(f'faves has {df_allfaves.shape[0]} rows.')
        print(f'tags has {df_alltags.shape[0]} rows.')
                
        print(f"Done with batch for page { cur_page } of { tot_pages }.")
        print(f'Time gathering details for page: {cur_page}, {time.time() - start}')
        time.sleep(1)
        return cur_page,tot_pages

    def get_user_photos(self):
        r = self._get_photo_batch(1)
        while r[0] < r[1]:
            r = self._get_photo_batch(r[0] + 1)

        print('Done.')

    def _test_photos(self):
        start = time.time()
        photos = self._flickr.people.getPhotos(user_id=self._userid, per_page=100, page=1)

        df2 = pd.DataFrame(photos['photos']['photo'])
        df_dates = pd.DataFrame()
        df_url = pd.DataFrame()
        for f in photos['photos']['photo']:
            photoid = f['id']
            phototitle = f['title']

            i1 = self._flickr.photos.getInfo(photo_id=photoid, format='parsed-json')
            df_tags = pd.DataFrame(i1['photo']['tags']['tag'])
            #photoids = ['photoid'] * range(df_tags.index.size)
            photoids = [photoid for i in range(df_tags.index.size)]
            df_tags['photo_id'] = photoids

            # df_dates = pd.DataFrame(i1['photo'])['dates'] # ['posted']
            # pd.DataFrame(i1[('id','photo')])['dates']  # ['posted']
            dfx = pd.DataFrame(i1['photo'])
            ### TODO This code snipet needs to be used in get_photos_stats when flattening the stats sublevel
            df_dates = df_dates.append(dfx.loc[['lastupdate'], ['id', 'dates']].append(dfx.loc[['posted', 'taken'], ['id', 'dates']]))
            # df_alldates = df_alldates.append(df_dates)
            # df_url = pd.DataFrame(i1['photo'])['urls']['url']
            df_url = df_url.append(pd.DataFrame(pd.DataFrame(i1['photo'])['urls']['url']))
            # df_allurls = df_allurls.append(df_url)

            #df_dates = df_dates.append(pd.DataFrame.from_dict(i1['photo']['dates'], orient="index").T)
            #df_dates['photoid'] = theid
            #df_dates = normalize(i1['photo'],['dates'])
            #df_comments = pd.DataFrame(i1['photo']['comments'])
            #df_comments = df_comments.append(pd.DataFrame.from_dict(i1['photo']['comments'], orient="index").T)

        print(df_dates.shape)
        print(f'Time in test proc {time.time() - start}')
        # pd.concat([df2, df_dates], axis=1)
        # df2 = df2.join(df_dates.shape)

    def _test_single_photo(self,photoid):
        
        i1 = self._flickr.photos.getInfo(photo_id=photoid, format='parsed-json')
        
        
        dfx = pd.DataFrame(i1['photo'])
        df_dates = pd.DataFrame()

        ### TODO This code snipet needs to be used in get_photos_stats when flattening the stats sublevel
        df_dates = df_dates.append(dfx.loc[['lastupdate'], ['id', 'dates']].append(dfx.loc[['posted', 'taken'], ['id', 'dates']]))
        dlu = df_dates.loc['lastupdate'][1]
        ts = time.strftime("%a, %d %b %Y %H:%M:%S %Z", time.localtime(int(dlu)))
        return ts
