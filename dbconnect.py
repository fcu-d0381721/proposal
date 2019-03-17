import pymysql.cursors

# 連接配置訊息
config = {
    'host': '127.0.0.1',
    'port': 3306,
    'user': 'root',
    'password': '',
    'db': 'YouBike',
    'charset': 'utf8',
    'cursorclass': pymysql.cursors.DictCursor,
}


class connectDB():
    now_code = ''

    def __init__(self):
        self.cursor = None
        self.connection = None
        self.connect()

    # create connection
    def connect(self):
        self.connection = pymysql.connect(**config)
        # check connection
        if self.connection:
            print("^___<")
            self.cursor = self.connection.cursor()
        # self.cursor.execute("SET NAMES utf8mb4")
        # self.cursor.execute("SET CHARACTER SET utf8mb4")
        # self.cursor.execute("SET character_set_connection = utf8mb4")

    def create(self, code):

        global now_code
        self.cursor.execute("SET NAMES utf8mb4;")
        # self.cursor.execute("DROP TABLE IF EXISTS %s" % code)
        sql = """CREATE TABLE %s (
                        sno INT NOT NULL AUTO_INCREMENT,
                        r_time  DATETIME,
                        r_st VARCHAR(50),
                        b_time DATETIME,
                        b_st VARCHAR(50),
                        r_name VARCHAR(50),
                        t_time VARCHAR(50),
                         PRIMARY KEY (sno))""" % code
        self.cursor.execute(sql)

        sql = """ALTER TABLE `%s` CHANGE `r_st` `r_st` VARCHAR(50) CHARACTER SET utf8mb4 COLLATE 
        utf8mb4_general_ci """ % code
        self.cursor.execute(sql)

        sql = """ALTER TABLE `%s` CHANGE `b_st` `b_st` VARCHAR(50) CHARACTER SET utf8mb4 COLLATE 
                utf8mb4_general_ci """ % code
        self.cursor.execute(sql)

        sql = """ALTER TABLE `%s` CHANGE `r_name` `r_name` VARCHAR(50) CHARACTER SET utf8mb4 COLLATE 
                utf8mb4_general_ci """ % code
        self.cursor.execute(sql)

        sql = """ALTER TABLE `%s` CHANGE `t_time` `t_time` VARCHAR(50) CHARACTER SET utf8mb4 COLLATE 
                        utf8mb4_general_ci """ % code
        self.cursor.execute(sql)

        now_code = code

    def insert_data(self, temp):

        global now_code
        # print(temp)

        sql = "INSERT INTO `" + now_code + "`(r_time, r_st, b_time, b_st, r_name, t_time) VALUES " + temp
        val = (now_code,temp)
        try:
            # print(r_time, r_st, b_time, b_st, b_name, t_time)
            self.connection.ping(reconnect=True)
            self.cursor.execute(sql)
            self.connection.commit()
        except:
            self.connect()
            self.connection.rollback()

    def query_data(self, sna):
        sql = "SELECT snc FROM sinformation WHERE sna ='%s'" % sna
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        return result

    def exit(self):
        self.connection.close()
