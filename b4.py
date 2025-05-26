import pandas as pd
import psycopg2

def LoadRatings(file_path):
    conn = psycopg2.connect(
        host="localhost",
        database="movielens_db",
        user="kien",
        password="123456",
        port="5432"
    )
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS Ratings;")
    cur.execute("""
        CREATE TABLE Ratings (
            UserID INT,
            MovieID INT,
            Rating FLOAT
        );
    """)
    conn.commit()

    with open(file_path, 'r') as file:
        for line in file:
            parts = line.strip().split("::")
            if len(parts) >= 3:
                user, movie, rating = int(parts[0]), int(parts[1]), float(parts[2])
                cur.execute("INSERT INTO Ratings (UserID, MovieID, Rating) VALUES (%s, %s, %s);",
                            (user, movie, rating))

    conn.commit()
    print("✅ Tải dữ liệu thành công.")

# Chạy hàm
LoadRatings("/home/lc13n/bai_tap_lon_CSDL_phan_tan/ml-10M100K/ratings.dat")
