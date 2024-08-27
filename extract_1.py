import os, gzip, json
import sqlite3

def drop_table(conn, name):
    stmt = f'drop table {name}'
    cur = conn.cursor()
    cur.execute(stmt)
    cur.close()        
	
def create_info(conn):	
    cur = conn.cursor()
    cr = ChunkReader('j228')
    count = 0

    for num in range(228000, 228009):
        iter = cr.get_records(num)
        print(' before insert? ')
        for i, (ndx, obj) in zip(range(1000000),iter):
            if 'text' in obj:
                print(obj)
                count = count + 1
                stmt = 'insert into twitter(created_at,id,id_str,source,truncated,is_quote_status,quote_count,reply_count,retweet_count,favorite_count,favorited,retweeted,filter_level,lang,timestamp_ms) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
                cur.execute(stmt, (obj['created_at'], obj['id'],obj['id_str'],obj['source'],obj['truncated'],obj['is_quote_status'],obj['quote_count'],obj['reply_count'],obj['retweet_count'],obj['favorite_count'],obj['favorited'], obj['retweeted'],obj['filter_level'],obj['lang'],obj['timestamp_ms']))
               
                if 'display_text_range' in obj:
            #       print(obj['display_text_range'])
                    stmt1 = f'''update twitter set display_text_range= '{obj["display_text_range"]}' where pid_twittwer = ?'''
                    cur.execute(stmt1, (count,))
                if obj['in_reply_to_status_id'] is not None:
                    stmt1a = 'update twitter set in_reply_to_status_id = ?,in_reply_to_status_id_str = ?,in_reply_to_user_id = ?,in_reply_to_user_id_str =?,in_reply_to_screen_name = ? where pid_twittwer = ?' 
                    cur.execute(stmt1a, (obj['in_reply_to_status_id'],obj['in_reply_to_status_id_str'],obj['in_reply_to_user_id'],obj['in_reply_to_user_id_str'],obj['in_reply_to_screen_name'],count))
                if 'possibly_sensitive' in obj:
                    stmt1b = f'''update twitter set possibly_sensitive= '{obj["possibly_sensitive"]}' where pid_twittwer = ?'''
                    cur.execute(stmt1b, (count,))
                if 'quoted_status_id' in obj:
                    stmt1c = f'''update twitter set quoted_status_id= '{obj["quoted_status_id"]}' where pid_twittwer = ?'''
                    cur.execute(stmt1c, (count,))                    
                if 'quoted_status_id_str' in obj:
                    stmt1d = f'''update twitter set quoted_status_id_str= '{obj["quoted_status_id_str"]}' where pid_twittwer = ?'''
                    cur.execute(stmt1d, (count,))   

            # user object
                stmtuser = 'insert into user(user_id,user_id_str,user_name,user_screen_name,user_protected,user_verified,user_followers_count, user_friends_count,user_listed_count,user_favourites_count,user_statuses_count,user_created_at,user_geo_enabled,user_contributors_enabled, user_profile_background_color,user_profile_background_image_url,user_profile_background_image_url_https,user_profile_background_tile,user_profile_link_color, user_profile_sidebar_border_color,user_profile_sidebar_fill_color,user_profile_text_color,user_profile_use_background_image,user_profile_image_url, user_profile_image_url_https,user_default_profile, fid_twittwer) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
                cur.execute(stmtuser, (obj['user']['id'],obj['user']['id_str'],obj['user']['name'],obj['user']['screen_name'],obj['user']['protected'],obj['user']['verified'],obj['user']['followers_count'],obj['user']['friends_count'],obj['user']['listed_count'],obj['user']['favourites_count'],obj['user']['statuses_count'],obj['user']['created_at'],obj['user']['geo_enabled'],obj['user']['contributors_enabled'],obj['user']['profile_background_color'],obj['user']['profile_background_image_url'],obj['user']['profile_background_image_url_https'],obj['user']['profile_background_tile'],obj['user']['profile_link_color'],obj['user']['profile_sidebar_border_color'],obj['user']['profile_sidebar_fill_color'],obj['user']['profile_text_color'],obj['user']['profile_use_background_image'],obj['user']['profile_image_url'],obj['user']['profile_image_url_https'],obj['user']['default_profile'],count))
                            
            # user attributes if may null location , url ,description; case1: all data are null do not insert(eg:time_zone) or case2: case2: data are false(0) are not insert
                if obj['user']['location'] is not None:
                    stmtuser1 = 'update user set user_location= ? where fid_twittwer = ?' 
                    cur.execute(stmtuser1, (obj['user']['location'],count))
                if obj['user']['url'] is not None:
                    stmtuser2 = 'update user set user_url= ? where fid_twittwer = ?' 
                    cur.execute(stmtuser2, (obj['user']['url'],count))    
                if obj['user']['description'] is not None:
                    stmtuser3 = 'update user set user_description= ? where fid_twittwer = ?' 
                    cur.execute(stmtuser3, (obj['user']['description'],count)) 
                if 'profile_banner_url' in obj:
                    stmtuser4 = f'''update user set user_profile_banner_url= '{obj["profile_banner_url"]}' where fid_twittwer = ?'''
                    cur.execute(stmtuser4, (count,))       

             # coordinates object=> have same value and means with geo.  
                if obj['coordinates'] is not None:
                    stmtgeo1 = 'insert into coordinates(fid_twittwer) values (?)'
                    cur.execute(stmtgeo1, (count,))
                    stmtgeo = f'''update coordinates set coordinates_coordinates= '{obj["coordinates"]["coordinates"]}', coordinates_type= ? where fid_twittwer = ?''' 
                    cur.execute(stmtgeo, (obj['coordinates']['type'],count,))                     
                    
             # place object may null             
                if obj['place'] is not None:
                    stmtplace1 = 'insert into place(fid_twittwer) values (?)'
                    cur.execute(stmtplace1, (count,))
                    stmtplace = f'''update place set place_id= ?, place_name= ?, place_place_type = ?, place_full_name = ?, place_country_code = ?, place_country = ?, place_url= ?, place_bounding_box_type = ?, place_bounding_box_coordinates = '{obj['place']['bounding_box']['coordinates']}' where fid_twittwer = ?''' 
                    cur.execute(stmtplace, (obj['place']['id'],obj['place']['name'],obj['place']['place_type'],obj['place']['full_name'],obj['place']['country_code'],obj['place']['country'],obj['place']['url'],obj['place']['bounding_box']['type'],count,))

             # entities and it's extend object
                if 'entities' in obj:
                    stmtentities = 'insert into entities(fid_twittwer) values (?)'
                    cur.execute(stmtentities, (count,))                
                    if 'hashtags' in obj['entities']:  
                        stmtentities1 = 'update entities set entities_hashtags= ? where fid_twittwer = ?'
                        cur.execute(stmtentities1,  (str(obj['entities']['hashtags']), count,))
                    if 'urls' in obj['entities']:
                        stmtentities2 = 'update entities set entities_urls= ? where fid_twittwer = ?'
                        cur.execute(stmtentities2,  (str(obj['entities']['urls']),count,))    
                    if 'symbols' in obj['entities']:   
                        stmtentities3 = 'update entities set entities_symbols= ? where fid_twittwer = ?'
                        cur.execute(stmtentities3,  (str(obj['entities']['symbols']),count,))
                    if 'media' in obj['entities']:
                        stmtentities4 = 'update entities set entities_media= ? where fid_twittwer = ?'
                        cur.execute(stmtentities4,  (str(obj['entities']['media']),count,))    
                    if 'user_mentions' in obj['entities']:
                        stmtentities5 = 'update entities set entities_user_mentions= ? where fid_twittwer = ?'
                        cur.execute(stmtentities5,  (str(obj['entities']['user_mentions']),count,))  
 
             # quoted_status_permalink object
                if 'quoted_status_permalink' in obj:   
                    stmtqsp1 = 'insert into quoted_status_permalink(fid_twittwer) values (?)'
                    cur.execute(stmtqsp1, (count,))                    
                    stmtqsp = 'update quoted_status_permalink set quoted_status_permalink_url= ?,quoted_status_permalink_expanded= ?,quoted_status_permalink_display= ? where fid_twittwer = ?'
                    cur.execute(stmtqsp,  (obj['quoted_status_permalink']['url'],obj['quoted_status_permalink']['expanded'],obj['quoted_status_permalink']['display'],count,))    
           
             # delete object
                if 'delete' in obj:
                    stmtdelete1 = 'insert into delete(fid_twittwer) values (?)'
                    cur.execute(stmtdelete1, (count,))                  
                    stmtdelete = 'update deleted set delete_status_id= ?,delete_status_id_str= ?,delete_status_user_id= ?,delete_status_user_id_str= ?,delete_timestamp_ms= ? where fid_twittwer = ?'
                    cur.execute(stmtdelete,  (obj['delete']['status']['id'],obj['delete']['status']['id_str'],obj['delete']['status']['user_id'],obj['delete']['status']['user_id_str'],obj['delete']['timestamp_ms'],count,))                
                                                                  
             # retweeted_status object =>Recursive
                if 'retweeted_status' in obj:
                    insertRecall(obj['retweeted_status'],count,cur)  
             # quoted_status object
                if 'quoted_status' in obj:
                    insertRecall(obj['quoted_status'],count,cur) 
              
    print(' after insert? ')	      
    cur.close()  
 

# recall  obj['retweeted_status'] or obj['quoted_status', parent_pid_twittwer
def insertRecall(obj,lastcount,cur):
                count = lastcount + 1
                stmt = 'insert into twitter(created_at,id,id_str,source,truncated,is_quote_status,quote_count,reply_count,retweet_count,favorite_count,favorited,retweeted,filter_level,lang,parent_pid_twittwer) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
                cur.execute(stmt, (obj['created_at'], obj['id'],obj['id_str'],obj['source'],obj['truncated'],obj['is_quote_status'],obj['quote_count'],obj['reply_count'],obj['retweet_count'],obj['favorite_count'],obj['favorited'], obj['retweeted'],obj['filter_level'],obj['lang'],lastcount,))
               
                if 'display_text_range' in obj:
                    stmt1 = f'''update twitter set display_text_range= '{obj["display_text_range"]}' where pid_twittwer = ?'''
                    cur.execute(stmt1, (count,))
                if obj['in_reply_to_status_id'] is not None:
                    stmt1a = 'update twitter set in_reply_to_status_id = ?,in_reply_to_status_id_str = ?,in_reply_to_user_id = ?,in_reply_to_user_id_str =?,in_reply_to_screen_name = ? where pid_twittwer = ?' 
                    cur.execute(stmt1a, (obj['in_reply_to_status_id'],obj['in_reply_to_status_id_str'],obj['in_reply_to_user_id'],obj['in_reply_to_user_id_str'],obj['in_reply_to_screen_name'],count))
                if 'possibly_sensitive' in obj:
                    stmt1b = f'''update twitter set possibly_sensitive= '{obj["possibly_sensitive"]}' where pid_twittwer = ?'''
                    cur.execute(stmt1b, (count,))
                if 'quoted_status_id' in obj:
                    stmt1c = f'''update twitter set quoted_status_id= '{obj["quoted_status_id"]}' where pid_twittwer = ?'''
                    cur.execute(stmt1c, (count,))                    
                if 'quoted_status_id_str' in obj:
                    stmt1d = f'''update twitter set quoted_status_id_str= '{obj["quoted_status_id_str"]}' where pid_twittwer = ?'''
                    cur.execute(stmt1d, (count,))   

            # user object
                stmtuser = 'insert into user(user_id,user_id_str,user_name,user_screen_name,user_protected,user_verified,user_followers_count, user_friends_count,user_listed_count,user_favourites_count,user_statuses_count,user_created_at,user_geo_enabled,user_contributors_enabled, user_profile_background_color,user_profile_background_image_url,user_profile_background_image_url_https,user_profile_background_tile,user_profile_link_color, user_profile_sidebar_border_color,user_profile_sidebar_fill_color,user_profile_text_color,user_profile_use_background_image,user_profile_image_url, user_profile_image_url_https,user_default_profile, fid_twittwer) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
                cur.execute(stmtuser, (obj['user']['id'],obj['user']['id_str'],obj['user']['name'],obj['user']['screen_name'],obj['user']['protected'],obj['user']['verified'],obj['user']['followers_count'],obj['user']['friends_count'],obj['user']['listed_count'],obj['user']['favourites_count'],obj['user']['statuses_count'],obj['user']['created_at'],obj['user']['geo_enabled'],obj['user']['contributors_enabled'],obj['user']['profile_background_color'],obj['user']['profile_background_image_url'],obj['user']['profile_background_image_url_https'],obj['user']['profile_background_tile'],obj['user']['profile_link_color'],obj['user']['profile_sidebar_border_color'],obj['user']['profile_sidebar_fill_color'],obj['user']['profile_text_color'],obj['user']['profile_use_background_image'],obj['user']['profile_image_url'],obj['user']['profile_image_url_https'],obj['user']['default_profile'],count))
                            
            # user attributes if may null location , url ,description; case1: all data are null do not insert(eg:time_zone) or case2: case2: data are false(0) are not insert
                if obj['user']['location'] is not None:
                    stmtuser1 = 'update user set user_location= ? where fid_twittwer = ?' 
                    cur.execute(stmtuser1, (obj['user']['location'],count))
                if obj['user']['url'] is not None:
                    stmtuser2 = 'update user set user_url= ? where fid_twittwer = ?' 
                    cur.execute(stmtuser2, (obj['user']['url'],count))    
                if obj['user']['description'] is not None:
                    stmtuser3 = 'update user set user_description= ? where fid_twittwer = ?' 
                    cur.execute(stmtuser3, (obj['user']['description'],count)) 
                if 'profile_banner_url' in obj:
                    stmtuser4 = f'''update user set user_profile_banner_url= '{obj["profile_banner_url"]}' where fid_twittwer = ?'''
                    cur.execute(stmtuser4, (count,))       

             # coordinates object
                if obj['coordinates'] is not None:
                    stmtgeo1 = 'insert into coordinates(fid_twittwer) values (?)'
                    cur.execute(stmtgeo1, (count,))
                    stmtgeo = f'''update coordinates set coordinates_coordinates= '{obj["coordinates"]["coordinates"]}', coordinates_type= ? where fid_twittwer = ?''' 
                    cur.execute(stmtgeo, (obj['coordinates']['type'],count,))                     
                    
             # place object may null             
                if obj['place'] is not None:
                    stmtplace1 = 'insert into place(fid_twittwer) values (?)'
                    cur.execute(stmtplace1, (count,))
                    stmtplace = f'''update place set place_id= ?, place_name= ?, place_place_type = ?, place_full_name = ?, place_country_code = ?, place_country = ?, place_url= ?, place_bounding_box_type = ?, place_bounding_box_coordinates = '{obj['place']['bounding_box']['coordinates']}' where fid_twittwer = ?''' 
                    cur.execute(stmtplace, (obj['place']['id'],obj['place']['name'],obj['place']['place_type'],obj['place']['full_name'],obj['place']['country_code'],obj['place']['country'],obj['place']['url'],obj['place']['bounding_box']['type'],count,))

             # entities and it's extend object
                if 'entities' in obj:
                    stmtentities = 'insert into entities(fid_twittwer) values (?)'
                    cur.execute(stmtentities, (count,))                
                    if 'hashtags' in obj['entities']:  
                        stmtentities1 = 'update entities set entities_hashtags= ? where fid_twittwer = ?'
                        cur.execute(stmtentities1,  (str(obj['entities']['hashtags']), count,))
                    if 'urls' in obj['entities']:
                        stmtentities2 = 'update entities set entities_urls= ? where fid_twittwer = ?'
                        cur.execute(stmtentities2,  (str(obj['entities']['urls']),count,))    
                    if 'symbols' in obj['entities']:   
                        stmtentities3 = 'update entities set entities_symbols= ? where fid_twittwer = ?'
                        cur.execute(stmtentities3,  (str(obj['entities']['symbols']),count,))
                    if 'media' in obj['entities']:
                        stmtentities4 = 'update entities set entities_media= ? where fid_twittwer = ?'
                        cur.execute(stmtentities4,  (str(obj['entities']['media']),count,))    
                    if 'user_mentions' in obj['entities']:
                        stmtentities5 = 'update entities set entities_user_mentions= ? where fid_twittwer = ?'
                        cur.execute(stmtentities5,  (str(obj['entities']['user_mentions']),count,))  
 
             # quoted_status_permalink object
                if 'quoted_status_permalink' in obj:   
                    stmtqsp1 = 'insert into quoted_status_permalink(fid_twittwer) values (?)'
                    cur.execute(stmtqsp1, (count,))                    
                    stmtqsp = 'update quoted_status_permalink set quoted_status_permalink_url= ?,quoted_status_permalink_expanded= ?,quoted_status_permalink_display= ? where fid_twittwer = ?'
                    cur.execute(stmtqsp,  (obj['quoted_status_permalink']['url'],obj['quoted_status_permalink']['expanded'],obj['quoted_status_permalink']['display'],count,))    
           
             # delete object
                if 'delete' in obj:
                    stmtdelete1 = 'insert into delete(fid_twittwer) values (?)'
                    cur.execute(stmtdelete1, (count,))                  
                    stmtdelete = 'update deleted set delete_status_id= ?,delete_status_id_str= ?,delete_status_user_id= ?,delete_status_user_id_str= ?,delete_timestamp_ms= ? where fid_twittwer = ?'
                    cur.execute(stmtdelete,  (obj['delete']['status']['id'],obj['delete']['status']['id_str'],obj['delete']['status']['user_id'],obj['delete']['status']['user_id_str'],obj['delete']['timestamp_ms'],count,))                
                                                                  
             # retweeted_status object
                if 'retweeted_status' in obj:
                    insertRecall(obj['retweeted_status'],count,cur)   
             # quoted_status object
                if 'quoted_status' in obj:
                    insertRecall(obj['quoted_status'],count,cur)       


def show_twitter(conn):
    cur = conn.cursor()
    stmt = 'select * from twitter limit 2'
    cur.execute(stmt)
    for r in cur:
        print(r)
    cur.close() 

def create_table(conn, name):
    if name == 'twitter':
        stmt = 'create table twitter(pid_twittwer INTEGER PRIMARY KEY,created_at,id,id_str,display_text_range,source,truncated,in_reply_to_status_id,in_reply_to_status_id_str,in_reply_to_user_id, in_reply_to_user_id_str,in_reply_to_screen_name,is_quote_status,quote_count,reply_count,retweet_count,favorite_count, favorited,retweeted,filter_level,lang,timestamp_ms, possibly_sensitive,quoted_status_id,quoted_status_id_str,parent_pid_twittwer)'
    if name == 'user':   
        stmt = 'create table user(pid_user INTEGER PRIMARY KEY,user_id,user_id_str,user_name,user_screen_name,user_location,user_url,user_description, user_protected,user_verified,user_followers_count,user_friends_count,user_listed_count,user_favourites_count,user_statuses_count,user_created_at, user_geo_enabled,user_contributors_enabled,user_profile_background_color,user_profile_background_image_url,user_profile_background_image_url_https, user_profile_background_tile,user_profile_link_color,user_profile_sidebar_border_color,user_profile_sidebar_fill_color,user_profile_text_color, user_profile_use_background_image,user_profile_image_url,user_profile_image_url_https,user_profile_banner_url,user_default_profile, fid_twittwer, FOREIGN KEY (fid_twittwer) REFERENCES twitter (pid_twittwer))' 
    if name == 'coordinates':
        stmt = 'create table coordinates(pid_coordinates INTEGER PRIMARY KEY, coordinates_coordinates,coordinates_type,pid_twittwer,fid_twittwer, FOREIGN KEY (fid_twittwer) REFERENCES twitter (pid_twittwer))'
    if name == 'place':
        stmt = 'create table place(pid_place INTEGER PRIMARY KEY,place_id,place_name,place_place_type,place_full_name,place_country_code,place_country,place_url, place_bounding_box_type, place_bounding_box_coordinates,fid_twittwer, FOREIGN KEY (fid_twittwer) REFERENCES twitter (pid_twittwer))'
    if name == 'entities':
        stmt = 'create table entities(pid_entities INTEGER PRIMARY KEY,entities_hashtags,entities_urls,entities_symbols,entities_media,entities_user_mentions,fid_twittwer, FOREIGN KEY (fid_twittwer) REFERENCES twitter (pid_twittwer))'                                                             
    if name == 'quoted_status_permalink':
        stmt = 'create table quoted_status_permalink(pid_quoted_status_permalink INTEGER PRIMARY KEY,quoted_status_permalink_url,quoted_status_permalink_expanded,quoted_status_permalink_display,fid_twittwer, FOREIGN KEY (fid_twittwer) REFERENCES twitter (pid_twittwer))'
    if name == 'deleted':
        stmt = 'create table deleted(pid_delete INTEGER PRIMARY KEY,delete_status_id,delete_status_id_str,delete_status_user_id, delete_status_user_id_str,delete_timestamp_ms,fid_twittwer, FOREIGN KEY (fid_twittwer) REFERENCES twitter (pid_twittwer))'
    cur = conn.cursor()
    cur.execute(stmt)
    cur.close() 
   

class ChunkReader:

    def __init__(self, cnkdir):
        self.__cnkdir = cnkdir      
    def get_cnkdir(self):
        return self.__cnkdir       
    def get_filename(self, cnk):
        cnkdir = self.get_cnkdir() 
        return f'{cnkdir}/a{cnk:08d}.cnk.gz'    	
    def get_records(self, cnk): 
        fn = self.get_filename(cnk)
        if not os.path.exists(fn):
            return
        with gzip.open(fn, 'rb') as f:
            res_bytes = f.read()

        lst = res_bytes.splitlines()
        res_bytes = None # to save intermediate memory
        for line in lst:
            record = eval(line)
            if record == [] or record is None:
                return
            ndx = record[0]
            yield ndx, json.loads(record[1])
if __name__ == '__main__':
    def test():
        conn = sqlite3.connect('test3.db')
        drop_table(conn, 'deleted')
        drop_table(conn, 'quoted_status_permalink')
        drop_table(conn, 'entities')
        drop_table(conn, 'place')
        drop_table(conn, 'coordinates')
        drop_table(conn, 'user')
        drop_table(conn, 'twitter')
        create_table(conn, 'twitter')
        create_table(conn, 'user')
        create_table(conn, 'coordinates')
        create_table(conn, 'place')
        create_table(conn, 'entities')
        create_table(conn, 'quoted_status_permalink')
        create_table(conn, 'deleted')
        create_info(conn)
        show_twitter(conn)
#        cr = ChunkReader('j228')
#        iter = cr.get_records(228000)
#        print('[')
#        for i, (ndx, obj) in zip(range(20),iter):
#            if 'text' in obj:
#                print(obj,",")
#        print(']')

        conn.commit()
        conn.close()
    test()
