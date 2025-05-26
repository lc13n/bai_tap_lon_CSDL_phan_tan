import psycopg2

RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'

# Hàm dùng để mở kết nối đến database
def getopenconnection(user='kien', password='123456', dbname='dds_assgn1'):
    return psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host='localhost'
    )

# 1. LOAD RATINGS
def loadratings(ratingstablename, filepath, openconnection):
    cur = openconnection.cursor()

    cur.execute(f"DROP TABLE IF EXISTS {ratingstablename};")
    cur.execute(f"""
        CREATE TABLE {ratingstablename} (
            userid INT,
            movieid INT,
            rating FLOAT
        );
    """)
    openconnection.commit()

    with open(filepath, 'r') as file:
        for line in file:
            parts = line.strip().split("::")
            if len(parts) >= 3:
                userid = int(parts[0])
                movieid = int(parts[1])
                rating = float(parts[2])
                cur.execute(f"""
                    INSERT INTO {ratingstablename} (userid, movieid, rating)
                    VALUES (%s, %s, %s);
                """, (userid, movieid, rating))

    openconnection.commit()
    cur.close()

# 2. RANGE PARTITION
def rangepartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    delta = 5.0 / numberofpartitions

    for i in range(numberofpartitions):
        cur.execute(f"DROP TABLE IF EXISTS {RANGE_TABLE_PREFIX}{i};")
        cur.execute(f"""
            CREATE TABLE {RANGE_TABLE_PREFIX}{i} (
                userid INT,
                movieid INT,
                rating FLOAT
            );
        """)
        min_val = i * delta
        max_val = min_val + delta

        if i == 0:
            cur.execute(f"""
                INSERT INTO {RANGE_TABLE_PREFIX}{i}
                SELECT * FROM {ratingstablename}
                WHERE rating >= %s AND rating <= %s;
            """, (min_val, max_val))
        else:
            cur.execute(f"""
                INSERT INTO {RANGE_TABLE_PREFIX}{i}
                SELECT * FROM {ratingstablename}
                WHERE rating > %s AND rating <= %s;
            """, (min_val, max_val))

    openconnection.commit()
    cur.close()

# 3. ROUND ROBIN PARTITION
def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()

    for i in range(numberofpartitions):
        cur.execute(f"DROP TABLE IF EXISTS {RROBIN_TABLE_PREFIX}{i};")
        cur.execute(f"""
            CREATE TABLE {RROBIN_TABLE_PREFIX}{i} (
                userid INT,
                movieid INT,
                rating FLOAT
            );
        """)

    cur.execute(f"SELECT * FROM {ratingstablename};")
    rows = cur.fetchall()

    for index, row in enumerate(rows):
        part_index = index % numberofpartitions
        cur.execute(f"""
            INSERT INTO {RROBIN_TABLE_PREFIX}{part_index}
            (userid, movieid, rating) VALUES (%s, %s, %s);
        """, row)

    openconnection.commit()
    cur.close()

# 4. RANGE INSERT
def rangeinsert(ratingstablename, userid, movieid, rating, openconnection):
    cur = openconnection.cursor()
    cur.execute(f"""
        INSERT INTO {ratingstablename} (userid, movieid, rating)
        VALUES (%s, %s, %s);
    """, (userid, movieid, rating))

    num_partitions = count_partitions(RANGE_TABLE_PREFIX, openconnection)
    delta = 5.0 / num_partitions
    index = int(rating / delta)

    if rating % delta == 0 and index != 0:
        index -= 1
    if index >= num_partitions:
        index = num_partitions - 1

    cur.execute(f"""
        INSERT INTO {RANGE_TABLE_PREFIX}{index}
        (userid, movieid, rating) VALUES (%s, %s, %s);
    """, (userid, movieid, rating))

    openconnection.commit()
    cur.close()

# 5. ROUND ROBIN INSERT
def roundrobininsert(ratingstablename, userid, movieid, rating, openconnection):
    cur = openconnection.cursor()
    cur.execute(f"""
        INSERT INTO {ratingstablename} (userid, movieid, rating)
        VALUES (%s, %s, %s);
    """, (userid, movieid, rating))

    num_partitions = count_partitions(RROBIN_TABLE_PREFIX, openconnection)
    cur.execute(f"SELECT COUNT(*) FROM {ratingstablename};")
    total_rows = cur.fetchone()[0]
    index = (total_rows - 1) % num_partitions

    cur.execute(f"""
        INSERT INTO {RROBIN_TABLE_PREFIX}{index}
        (userid, movieid, rating) VALUES (%s, %s, %s);
    """, (userid, movieid, rating))

    openconnection.commit()
    cur.close()

# Utility: Count how many partitions exist
def count_partitions(prefix, openconnection):
    cur = openconnection.cursor()
    cur.execute(f"""
        SELECT COUNT(*) FROM pg_stat_user_tables
        WHERE relname LIKE %s;
    """, (prefix + '%',))
    count = cur.fetchone()[0]
    cur.close()
    return count
