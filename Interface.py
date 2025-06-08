#!/usr/bin/env python3

import psycopg2
import psycopg2.extensions
import os
from io import StringIO
from itertools import islice

def getopenconnection(user='postgres', password='1234',
                      dbname='dds_assgn1', host='localhost', port='5432'):
    """
    Trả về connection đến database dds_assgn1.
    """
    conn = psycopg2.connect(dbname=dbname,
                            user=user,
                            password=password,
                            host=host,
                            port=port)
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    return conn

def create_db(dbname='dds_assgn1'):
    """
    Tạo database nếu chưa tồn tại.
    """
    con = psycopg2.connect(dbname='postgres', user='postgres',
                           password='1234', host='localhost')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=%s",
        (dbname,)
    )
    if cur.fetchone()[0] == 0:
        cur.execute(f"CREATE DATABASE {dbname}")
    cur.close()
    con.close()

def loadratings(ratingstablename, ratingsfilepath, openconnection):
    """
    Tạo bảng ratingstablename và load dữ liệu từ file format userID::movieID::rating::timestamp
    """
    cur = openconnection.cursor()
    try:
        cur.execute(f"DROP TABLE IF EXISTS {ratingstablename}")
        cur.execute(
            f"CREATE TABLE {ratingstablename} ("
            "userid INT NOT NULL, movieid INT, rating REAL, timestamp BIGINT)"
        )
        # copy in batches
        with open(ratingsfilepath) as f:
            for batch in iter(lambda: tuple(islice(f, 5000)), ()):
                buf = StringIO()
                buf.write(''.join(line.replace('::', ',') for line in batch))
                buf.seek(0)
                cur.copy_from(buf, ratingstablename,
                              sep=',',
                              columns=('userid','movieid','rating','timestamp'))
        # nếu không cần timestamp thì drop
        cur.execute(f"ALTER TABLE {ratingstablename} DROP COLUMN timestamp")
        openconnection.commit()
    except:
        openconnection.rollback()
        raise
    finally:
        cur.close()

def rangepartition(ratingstablename, numberofpartitions, openconnection):
    """
    Partition theo range: tạo range_part0..range_part{n-1}
    """
    cur = openconnection.cursor()
    try:
        # xóa các partition cũ
        for i in range(numberofpartitions):
            cur.execute(f"DROP TABLE IF EXISTS range_part{i}")
        # tạo và insert lại theo range
        interval = 5.0 / numberofpartitions
        for i in range(numberofpartitions):
            low = i * interval
            high = (i + 1) * interval if i < numberofpartitions - 1 else 5.0
            if i == 0:
                cond = f"rating >= {low} AND rating <= {high}"
            else:
                cond = f"rating > {low} AND rating <= {high}"
            cur.execute(
                f"CREATE TABLE range_part{i} AS "
                f"SELECT userid, movieid, rating FROM {ratingstablename} WHERE {cond}"
            )
        openconnection.commit()
    except:
        openconnection.rollback()
        raise
    finally:
        cur.close()

def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    """
    Partition theo Round-Robin: tạo rrobin_part0..rrobin_part{n-1}
    Và reset file counter để roundrobininsert sau này bắt đầu tại part0.
    """
    cur = openconnection.cursor()
    try:
        # reset counter
        if os.path.exists('.rr_counter'):
            os.remove('.rr_counter')
        # xóa cũ
        for i in range(numberofpartitions):
            cur.execute(f"DROP TABLE IF EXISTS rrobin_part{i}")
        openconnection.commit()

        # tạo bảng tạm để gán idx
        cur.execute(
            f"SELECT userid, movieid, rating, "
            f"ROW_NUMBER() OVER () - 1 AS idx "
            f"INTO temp_rr FROM {ratingstablename}"
        )
        # sinh partition
        for i in range(numberofpartitions):
            cur.execute(
                f"CREATE TABLE rrobin_part{i} AS "
                f"SELECT userid, movieid, rating FROM temp_rr WHERE idx % {numberofpartitions} = {i}"
            )
        cur.execute("DROP TABLE temp_rr")
        openconnection.commit()
    except:
        openconnection.rollback()
        raise
    finally:
        cur.close()

def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Insert 1 record vào đúng bảng range_partX dựa vào rating.
    Điều kiện giống rangepartition.
    """
    cur = openconnection.cursor()
    try:
        # tìm các partition hiện có
        cur.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema='public' AND table_name LIKE 'range_part%%'"
        )
        parts = sorted(r[0] for r in cur.fetchall())
        n = len(parts)
        interval = 5.0 / n

        # xác định bảng đích dựa vào điều kiện
        for i, tbl in enumerate(parts):
            low = i * interval
            high = (i + 1) * interval if i < n - 1 else 5.0
            if (i == 0 and low <= rating <= high) or (i > 0 and low < rating <= high):
                cur.execute(
                    f"INSERT INTO {tbl} (userid, movieid, rating) VALUES (%s, %s, %s)",
                    (userid, itemid, rating)
                )
                break

        openconnection.commit()
    except:
        openconnection.rollback()
        raise
    finally:
        cur.close()

def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Insert 1 record vào đúng rrobin_partX theo Round-Robin.
    Dùng file '.rr_counter' để tracking lượt chèn.
    """
    cur = openconnection.cursor()
    try:
        # lấy danh sách partition theo thứ tự tên
        cur.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema='public' AND table_name LIKE 'rrobin_part%%' "
            "ORDER BY table_name"
        )
        parts = [r[0] for r in cur.fetchall()]
        n = len(parts)

        # đọc/ghi counter
        counter_file = '.rr_counter'
        try:
            with open(counter_file, 'r+') as f:
                cnt = int(f.read() or '0')
                idx = cnt % n
                f.seek(0); f.write(str(cnt + 1)); f.truncate()
        except FileNotFoundError:
            idx = 0
            with open(counter_file, 'w') as f:
                f.write('1')

        target = parts[idx]
        cur.execute(
            f"INSERT INTO {target} (userid, movieid, rating) VALUES (%s, %s, %s)",
            (userid, itemid, rating)
        )
        openconnection.commit()
    except:
        openconnection.rollback()
        raise
    finally:
        cur.close()

def deleteallpublictables(openconnection):
    """
    Xóa tất cả bảng trong public schema.
    """
    cur = openconnection.cursor()
    cur.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema='public'"
    )
    for (tbl,) in cur.fetchall():
        cur.execute(f"DROP TABLE IF EXISTS {tbl} CASCADE")
    openconnection.commit()
    cur.close()
