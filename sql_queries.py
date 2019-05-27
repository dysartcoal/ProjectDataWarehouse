import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = ""
staging_songs_table_drop = ""
songplay_table_drop = ""
user_table_drop = ""
song_table_drop = ""
artist_table_drop = ""
time_table_drop = ""

# CREATE TABLES

staging_events_table_create= ("""
""")

staging_songs_table_create = ("""
""")

# songplays - records in event data associated with song plays i.e. records with page NextSong
# songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays
        (songplay_id bigint IDENTITY(0,1),
        start_time bigint NOT NULL,   
        user_id int NOT NULL,
        level varchar(4) NOT NULL,
        song_id varchar(20) NOT NULL, 
        artist_id varchar(20) NOT NULL,
        session_id int NOT NULL,
        location varchar(60),
        user_agent varchar(4096) NOT NULL)
""")

# users - users in the app
# user_id, first_name, last_name, gender, level
user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users
        (user_id int NOT NULL,
        first_name varchar(30) NOT NULL,
        last_name varchar(30) NOT NULL,
        gender varchar(1) NOT NULL,
        level varchar(4) NOT NULL)
        disttyle ALL
""") 

# songs - songs in music database
# song_id, title, artist_id, year, duration
song_table_create = ("""
    CREATE TABLE IF NOT EXISTS users
        (song_id int NOT NULL,
        title varchar(100) NOT NULL,
        artist_id int NOT NULL,
        year int,
        duration int)
""")

#artists - artists in music database
#artist_id, name, location, lattitude, longitude
artist_table_create = ("""
""") diststyle ALL;

#time - timestamps of records in songplays broken down into specific units
#start_time, hour, day, week, month, year, weekday
time_table_create = ("""
""")

# STAGING TABLES

staging_events_copy = ("""
""").format()

staging_songs_copy = ("""
""").format()

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
