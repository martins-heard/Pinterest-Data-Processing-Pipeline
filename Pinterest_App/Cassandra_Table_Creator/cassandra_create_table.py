from cassandra.cluster import Cluster

new_cluster = Cluster()
session = new_cluster.connect('data')

# session.execute(''' DROP TABLE data.pinterest_data;''')

session.execute(''' CREATE TABLE pinterest_data_2 (
    category text,
    unique_id text,
    title text,
    description text,
    follower_count int,
    tag_list text,
    is_image_or_video boolean,
    image_src text,
    downloaded int,
    save_location text,
    PRIMARY KEY (unique_id)
    );''')