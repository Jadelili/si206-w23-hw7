# Your name: Xiaoyi Li
# Your student id: 20937100
# Your email: jadeli@umich.edu
# List who you have worked with on this project: None

import unittest
import sqlite3
import json
import os

def read_data(filename):
    full_path = os.path.join(os.path.dirname(__file__), filename)
    f = open(full_path)
    file_data = f.read()
    f.close()
    json_data = json.loads(file_data)   # already a dict
    return json_data

def open_database(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db_name)
    cur = conn.cursor()
    return cur, conn

def make_positions_table(data, cur, conn):
    positions = []
    for player in data['squad']:
        position = player['position']
        if position not in positions:
            positions.append(position)
    cur.execute("CREATE TABLE IF NOT EXISTS Positions (id INTEGER PRIMARY KEY, position TEXT UNIQUE)")   # why unique?
    for i in range(len(positions)):
        cur.execute("INSERT OR IGNORE INTO Positions (id, position) VALUES (?,?)",(i, positions[i]))
    conn.commit()


def make_players_table(data, cur, conn):
    cur.execute('''DROP TABLE IF EXISTS Players''')
    cur.execute('''CREATE TABLE Players (id INTEGER PRIMARY KEY, name TEXT, position_id INTEGER,
    birthyear INTEGER, nationality TEXT)''')
    # cur.execute('''CREATE TABLE IF NOT EXISTS Players (id INTEGER PRIMARY KEY, name TEXT, position_id INTEGER,
    # birthyear INTEGER, nationality TEXT)''')


    for i in data["squad"]:
        p_id = i["id"]     # already an integer
        p_name = i["name"]
        p_pos = i["position"]
        p_birth = int(i["dateOfBirth"][:4])
        p_nat = i["nationality"]

        cur.execute('SELECT id FROM Positions WHERE position = ?', (p_pos, ))
        pos_id = int(cur.fetchone()[0])
        cur.execute('''INSERT OR IGNORE INTO Players (id, name, position_id, birthyear, nationality)
                            VALUES(?,?,?,?,?)''', (p_id, p_name, pos_id, p_birth, p_nat))
    conn.commit()


def nationality_search(countries, cur, conn):
    country_list = []
    for country in countries:
        # cur.execute(f"SELECT name, position_id, nationality FROM Players WHERE nationality = {country}")
        cur.execute("SELECT name, position_id, nationality FROM Players WHERE nationality = ?", (country,))
        country_list.extend(cur.fetchall())
    return country_list


def birthyear_nationality_search(age, country, cur, conn):
    birth_n_list = []
    age_year = 2023 - age
    cur.execute("SELECT name, nationality, birthyear FROM Players WHERE birthyear < ? AND nationality = ?", (age_year, country))
    birth_n_list.extend(cur.fetchall())
    return birth_n_list


## [TASK 4]: 15 points
def position_birth_search(position, age, cur, conn):
    p_birth_list = []
    age_year = 2023 - age
    cur.execute('''SELECT Players.name, Positions.position, Players.birthyear FROM Players JOIN Positions
    ON Players.position_id = Positions.id WHERE Players.birthyear > ? AND Positions.position = ?''', (age_year, position))
    p_birth_list.extend(cur.fetchall())
    return p_birth_list


# [EXTRA CREDIT]
def make_winners_table(data, cur, conn):
    cur.execute('''DROP TABLE IF EXISTS Winners''')
    cur.execute('''CREATE TABLE Winners (id INTEGER PRIMARY KEY, name TEXT UNIQUE)''')
    # cur.execute("CREATE TABLE IF NOT EXISTS Winners (id INTEGER PRIMARY KEY, name TEXT UNIQUE)")  # not gonna make changed to the table

    id_list = []
    name_list = []
    for season_dict in data['seasons']:
        if season_dict['winner'] != None:
            w_id = season_dict['winner']['id']
            if w_id not in id_list:
                id_list.append(w_id)
            
            w_name = season_dict['winner']['name']
            if w_name not in name_list:
                name_list.append(w_name)

    for i in range(len(id_list)):
        # I saw on Piazza that we are asked to put consecutive in id column
        cur.execute('''INSERT OR IGNORE INTO Winners (id, name) VALUES(?,?)''', (id_list[i], name_list[i]))
        # if id is consecutive:
        # cur.execute('''INSERT OR IGNORE INTO Winners (id, name) VALUES(?,?)''', (i, name_list[i]))  # leads to dif sequence of rows
    conn.commit()

    # for season_dict in data['seasons']:
    #     w_id = season_dict['winner']['id']
    #     if season_dict['winner'] != None:
    #         w_name = season_dict['winner']['name']
    #         
    #         exp: w_id: 27 pieces; w_name: 8 pieces. But since we ignore, the 21 pieces are not gonna added (given the same team name and id)
    #         cur.execute('''INSERT OR IGNORE INTO Winners (id, name) VALUES(?,?)''', (w_id, w_name))
    # conn.commit()


def make_seasons_table(data, cur, conn):
    cur.execute('''DROP TABLE IF EXISTS Seasons''')
    # cur.execute('''CREATE TABLE Seasons (id INTEGER PRIMARY KEY, winner_id TEXT, end_year INTEGER)''')
    cur.execute('''CREATE TABLE Seasons (id INTEGER PRIMARY KEY, winner_id TEXT UNIQUE, end_year INTEGER UNIQUE)''')
    
    id_list = []
    winner_id_list = []
    end_year_list = []
    for season_dict in data['seasons']:
        if season_dict['winner'] != None:
            s_id = season_dict['id']
            id_list.append(s_id)
            # if s_id not in id_list:
            #     id_list.append(s_id)
            
            w_id = str(season_dict['winner']['id'])
            winner_id_list.append(w_id)
            # if w_id not in winner_id_list:
            #     winner_id_list.append(w_id)

            end_y = int(season_dict['endDate'][:4])
            end_year_list.append(end_y)
            # if end_y not in end_year_list:
            #     end_year_list.append(end_y)

    for i in range(len(id_list)):
        cur.execute('''INSERT OR IGNORE INTO Seasons (id, winner_id, end_year) VALUES(?,?,?)''', 
                    (id_list[i], winner_id_list[i], end_year_list[i]))
    conn.commit()


def winners_since_search(year, cur, conn):
    pass


class TestAllMethods(unittest.TestCase):
    def setUp(self):
        path = os.path.dirname(os.path.abspath(__file__))
        self.conn = sqlite3.connect(path+'/'+'Football.db')
        self.cur = self.conn.cursor()
        self.conn2 = sqlite3.connect(path+'/'+'Football_seasons.db')
        self.cur2 = self.conn2.cursor()

    def test_players_table(self):
        self.cur.execute('SELECT * from Players')
        players_list = self.cur.fetchall()

        self.assertEqual(len(players_list), 30)
        self.assertEqual(len(players_list[0]),5)
        self.assertIs(type(players_list[0][0]), int)
        self.assertIs(type(players_list[0][1]), str)
        self.assertIs(type(players_list[0][2]), int)
        self.assertIs(type(players_list[0][3]), int)
        self.assertIs(type(players_list[0][4]), str)

    def test_nationality_search(self):
        x = sorted(nationality_search(['England'], self.cur, self.conn))
        self.assertEqual(len(x), 11)
        self.assertEqual(len(x[0]), 3)
        self.assertEqual(x[0][0], "Aaron Wan-Bissaka")

        y = sorted(nationality_search(['Brazil'], self.cur, self.conn))
        self.assertEqual(len(y), 3)
        self.assertEqual(y[2],('Fred', 2, 'Brazil'))
        self.assertEqual(y[0][1], 3)

    def test_birthyear_nationality_search(self):
        a = birthyear_nationality_search(24, 'England', self.cur, self.conn)
        self.assertEqual(len(a), 7)
        self.assertEqual(a[0][1], 'England')
        self.assertEqual(a[3][2], 1992)
        self.assertEqual(len(a[1]), 3)

    def test_type_speed_defense_search(self):
        b = sorted(position_birth_search('Goalkeeper', 35, self.cur, self.conn))
        self.assertEqual(len(b), 2)
        self.assertEqual(type(b[0][0]), str)
        self.assertEqual(type(b[1][1]), str)
        self.assertEqual(len(b[1]), 3) 
        self.assertEqual(b[1], ('Jack Butland', 'Goalkeeper', 1993)) 

        c = sorted(position_birth_search("Defence", 23, self.cur, self.conn))
        self.assertEqual(len(c), 1)
        self.assertEqual(c, [('Teden Mengi', 'Defence', 2002)])
    
    # test extra credit
    def test_make_winners_table(self):
        self.cur2.execute('SELECT * from Winners')
        winners_list = self.cur2.fetchall()

        self.assertEqual(len(winners_list), 7)
        self.assertEqual(len(winners_list[0]),2)
        self.assertIs(type(winners_list[0][0]), int)
        self.assertIs(winners_list[2][0], 61)                 #[row][column]
        self.assertIs(type(winners_list[0][1]), str)
        self.assertEqual(winners_list[2][1], 'Chelsea FC')    # dif: is and equal?

    # def test_make_seasons_table(self):
    #     self.cur2.execute('SELECT * from Seasons')
    #     seasons_list = self.cur2.fetchall()

        pass

    def test_winners_since_search(self):

        pass


def main():

    #### FEEL FREE TO USE THIS SPACE TO TEST OUT YOUR FUNCTIONS

    json_data = read_data('football.json')
    cur, conn = open_database('Football.db')
    make_positions_table(json_data, cur, conn)
    make_players_table(json_data, cur, conn)
    conn.close()


    seasons_json_data = read_data('football_PL.json')
    cur2, conn2 = open_database('Football_seasons.db')
    make_winners_table(seasons_json_data, cur2, conn2)
    make_seasons_table(seasons_json_data, cur2, conn2)
    conn2.close()


if __name__ == "__main__":
    main()
    # unittest.main(verbosity = 2)
