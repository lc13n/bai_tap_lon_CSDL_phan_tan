import psycopg2

def Range_Partition(conn, n):
    cur = conn.cursor()

    # Bước 1: Xóa bảng phân mảnh cũ nếu có
    for i in range(n):
        cur.execute(f"DROP TABLE IF EXISTS range_part{i};")
        cur.execute(f"""
            CREATE TABLE range_part{i} (
                UserID INT,
                MovieID INT,
                Rating FLOAT
            );
        """)

    # Bước 2: Tính khoảng chia đều
    step = 5 / n

    # Bước 3: Lấy toàn bộ dữ liệu từ bảng Ratings
    cur.execute("SELECT * FROM Ratings;")
    rows = cur.fetchall()

    # Bước 4: Phân phối dữ liệu vào các bảng range_part{i}
    for row in rows:
        user_id, movie_id, rating = row
        index = int(rating // step)

        # Nếu rating = 5.0 thì index = n, nên cần giảm về n - 1
        if index >= n:
            index = n - 1

        cur.execute(
            f"INSERT INTO range_part{index} (UserID, MovieID, Rating) VALUES (%s, %s, %s);",
            (user_id, movie_id, rating)
        )

    conn.commit()
    print(f"Đã phân mảnh bảng Ratings thành {n} bảng range_part0 đến range_part{n-1}.")



if __name__ == "__main__":
    try:
        # Kết nối tới PostgreSQL
        conn = psycopg2.connect(
            host="localhost",
            database="movielens_db",
            user="kien",
            password="123456",
            port="5432"
        )

        # Gọi hàm chia 3 phân mảnh (ví dụ)
        Range_Partition(conn, 3)

    except Exception as e:
        print("Lỗi khi kết nối hoặc thực thi:", e)

    finally:
        if conn:
            conn.close()
