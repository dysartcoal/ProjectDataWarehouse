import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
DWH_IAM_ROLE_NAME   = config.get("IAM_ROLE", "ARN")
SONGS_MANIFEST      = config.get("MANIFEST", "SONGS_MANIFEST")
LOG_JSONPATH        = config.get("S3", "LOG_JSONPATH")
LOG_DATA            = config.get("S3", "LOG_DATA")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events
        (artist_name varchar(1000) ENCODE ZSTD,
        auth varchar(20),
        first_name varchar(100),
        gender char,
        itemInSession int,
        last_name varchar(100),
        length decimal(8,3),
        level varchar(4),
        location varchar(1000),
        method varchar(10),
        page varchar(30),
        registration bigint,
        session_id int,
        song_title varchar(1000) ENCODE ZSTD,
        status int,
        start_time bigint,
        user_agent varchar(4096) ENCODE ZSTD,
        user_id int)
    SORTKEY(start_time)
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs
        (num_songs int,
        artist_id varchar(20) ,
        latitude decimal(9,6),
        longitude decimal(9,6),
        location varchar(1000),
        artist_name varchar(1000) ENCODE ZSTD,
        song_id varchar(20),
        title varchar(1000) ENCODE ZSTD,
        duration numeric(8,4),
        year int)
    DISTKEY(song_id)
    SORTKEY(song_id)
        
""")

# songplays - records in event data associated with song plays i.e. records with page NextSong
# songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays
        (songplay_id bigint IDENTITY(0,1),
        start_time timestamp NOT NULL,   
        user_id int,
        level varchar(4) ENCODE ZSTD,
        song_id varchar(20), 
        artist_id varchar(20) ENCODE ZSTD,
        session_id int,
        location varchar(60) ENCODE ZSTD,
        user_agent varchar(4096) ENCODE ZSTD,
        PRIMARY KEY(songplay_id),
        FOREIGN KEY(start_time) REFERENCES time(start_time),
        FOREIGN KEY(user_id) REFERENCES users(user_id),
        FOREIGN KEY(song_id) REFERENCES songs(song_id),
        FOREIGN KEY(artist_id) REFERENCES artists(artist_id))
    DISTKEY(song_id)
    SORTKEY(song_id, start_time)
""")

# users - users in the app
# user_id, first_name, last_name, gender, level
user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users
        (user_id int NOT NULL,
        first_name varchar(100),
        last_name varchar(100),
        gender char,
        level varchar(4),
        PRIMARY KEY(user_id))
    DISTSTYLE ALL
    SORTKEY(user_id)
""") 

# songs - songs in music database
# song_id, title, artist_id, year, duration
song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs
        (song_id varchar(20) NOT NULL,
        title varchar(500) ENCODE ZSTD,
        artist_id varchar(20) NOT NULL ENCODE ZSTD,
        year int ENCODE ZSTD,
        duration numeric(8,4) ENCODE ZSTD,
        PRIMARY KEY(song_id))
    DISTKEY(song_id)
    SORTKEY(song_id)
""")

#artists - artists in music database
#artist_id, name, location, lattitude, longitude
artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists
        (artist_id varchar(20) NOT NULL,
        name varchar(500) ENCODE ZSTD,
        location varchar(60),
        latitude decimal(9,6),
        longitude decimal(9,6),
        PRIMARY KEY(artist_id))
    DISTSTYLE ALL
    SORTKEY(artist_id)
""") 

#time - timestamps of records in songplays broken down into specific units
# start_time, hour, day, week, month, year, weekday
time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time
        (start_time timestamp NOT NULL,
        hour int,
        day int ,
        week int,
        month int,
        year int ,
        weekday varchar(10),
        PRIMARY KEY(start_time))
    SORTKEY(start_time)
""")

# STAGING TABLES
# Uses the prefix method to load files in parallel
staging_events_copy = ("""
    COPY staging_events 
    FROM {}
    IAM_ROLE {}
    JSON {} REGION 'us-west-2'
    STATUPDATE OFF
    COMPUPDATE OFF
    MAXERROR 10
""").format(LOG_DATA, DWH_IAM_ROLE_NAME, LOG_JSONPATH)

# Uses the manifest file method to load files in parallel
staging_songs_copy = ("""
    COPY staging_songs 
    FROM {} 
    IAM_ROLE {} 
    manifest 
    JSON 'auto' 
    REGION 'us-west-2'
    STATUPDATE OFF
    COMPUPDATE OFF
    MAXERROR 10
""").format(SONGS_MANIFEST, DWH_IAM_ROLE_NAME)

# FINAL TABLES

# A left join with a combined song and artist table 
# ensures that any song play entries for which there is 
# no matching song title in the songs table are still 
# captured.  The combined song and artist table ensure 
# that there is no match with the same song title for 
# a different artist or the same artist for a different
# song.
#
# Ignore entries from the logs which are not related
# to song plays by comparing the log.page and log.method.
songplay_table_insert = ("""
    INSERT INTO songplays (start_time,
                            user_id,
                            level,
                            song_id,
                            artist_id,
                            session_id,
                            location,
                            user_agent)
    (SELECT log.ts, 
            log.user_id, 
            log.level, 
            st_songs.song_id, 
            st_songs.artist_id, 
            log.session_id, 
            log.location, 
            log.user_agent
      FROM (SELECT TIMESTAMP 'epoch' + start_time/1000 * interval '1 second' AS ts, 
            *
            FROM staging_events
            WHERE page='NextSong') log
      LEFT JOIN staging_songs st_songs
      ON lower(log.song_title) = lower(st_songs.title)
      AND lower(log.artist_name) = lower(st_songs.artist_name)
      AND ROUND(log.length) = ROUND(st_songs.duration) --Don't lose data in the join due to small data discrepancies)
""")

# Ensure that there are no null user_ids and user_id is unique
user_table_insert = ("""
    INSERT INTO users
    (SELECT user_id, 
            max(first_name),
            max(last_name),
            max(gender),
            max(level)
    FROM staging_events 
    WHERE user_id is not NULL 
    GROUP BY user_id)   -- Ensure primary key is unique
""")

# ensure song_id is unique
song_table_insert = ("""
    INSERT INTO songs
    (SELECT st_songs.song_id,
            max(st_songs.title),
            max(st_songs.artist_id), 
            max(st_songs.year), 
            max(st_songs.duration)
    FROM staging_songs st_songs
    GROUP BY st_songs.song_id)  -- Ensure primary key is unique
""")

# ensure artist_id is unique
artist_table_insert = ("""
    INSERT INTO artists
    (SELECT st_songs.artist_id,
        max(st_songs.artist_name),
        max(st_songs.location),
        max(st_songs.latitude),
        max(st_songs.longitude)
    FROM staging_songs st_songs
    GROUP BY st_songs.artist_id) -- Ensure primary key is unique
""")

# Select only the times for log entries related to song plays
# by filtering on log.page and log.method.
# Ensure start_time values are unique.
time_table_insert = ("""
    INSERT INTO time
    (SELECT sp.start_time,
        DATEPART(h, sp.start_time),
        DATEPART(d, sp.start_time),
        DATEPART(w, sp.start_time),
        DATEPART(mon, sp.start_time),
        DATEPART(y, sp.start_time),
        CASE DATEPART(dow, sp.start_time)
            WHEN 0 THEN 'Sunday'
            WHEN 1 THEN 'Monday'
            WHEN 2 THEN 'Tuesday'
            WHEN 3 THEN 'Wednesday'
            WHEN 4 THEN 'Thursday'
            WHEN 5 THEN 'Friday'
            WHEN 6 THEN 'Saturday'
        END
    FROM songplays sp)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_songs_copy, staging_events_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

