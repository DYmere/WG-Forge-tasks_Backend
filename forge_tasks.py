#! /usr/bin/env python

__doc__   = "WG forge backend test"
__author__ = "Dzmitry Yatsyna"

import argparse
import psycopg2
import psycopg2.extras as ex
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib import parse
import json
import statistics

parser = argparse.ArgumentParser(prog="forge_tasks.py", description="Wargaming forge tasks", usage="./forge_tasks.py [-h] [--dbhost <DB HOST>] [--dbport <DB PORT>] [--dbname <DB NAME>] [--dbuser <DB USER>] [--dbpass <DB PASS>] [--shost <SERVER HOST>] [--sport <SERVER PORT>] [--max <COUNT>]", epilog="examples:\n\t./forge_tasks.py\n\t./forge_tasks.py --dbport 9000", formatter_class=argparse.RawTextHelpFormatter)
parser.add_argument("--dbhost", action="store", dest="DB_HOST", help="database host address (default: 'localhost')", default="localhost")
parser.add_argument("--dbport", action="store", dest="DB_PORT", help="database port number (default: '5432')", default="5432")
parser.add_argument("--dbname", action="store", dest="DB_NAME", help="database name (default: 'wg_forge_db')", default="wg_forge_db")
parser.add_argument("--dbuser", action="store", dest="DB_USER", help="database username (default: 'wg_forge')", default="wg_forge")
parser.add_argument("--dbpass", action="store", dest="DB_PASS", help="database user password (default: '42a')", default="5432")
parser.add_argument("--shost", action="store", dest="HTTP_HOST", help="HTTP server host address (default: 'localhost')", default="localhost")
parser.add_argument("--sport", action="store", dest="HTTP_PORT", help="HTTP server port number (default: '8080')", default="8080", type=int)
parser.add_argument("--max", action="store", dest="MAX_REQ", help="Maximum requests count (default: '10')", default="10", type=int)
args = parser.parse_args()

class ForgeHandler(BaseHTTPRequestHandler):
    """
    The handler parse the request and the headers, then call a method specific to the request type
    """
    def do_GET(self):
        """
        'GET' request serving function
        """
        global st_point, cur_request
        if time.time() - st_point < 1 and cur_request > args.MAX_REQ:
            self.send_response(429)
            self.send_header("Content-type","text/html")
            self.end_headers()
            time.sleep(0.2)
            return
        elif time.time() - st_point > 1:
            st_point = time.time()
            cur_request = 1
        self.func_PARSE()
        if self.parsed_url[2] in ["/ping", "/cats"]:
            self.func_DO()
        else:
            self.send_response(400)
            text="<h1 align=center>Bad request</h1>"
            self.func_PRINT(text)
    def do_POST(self):
        """
        POST" request serving function
        """
        global st_point, cur_request
        if time.time() - st_point < 1 and cur_request > args.MAX_REQ:
            self.send_response(429)
            self.send_header("Content-type","text/html")
            self.end_headers()
            time.sleep(0.2)
            return
        elif time.time() - st_point > 1:
            st_point = time.time()
            cur_request = 1
        self.func_PARSE()
        if self.parsed_url[2] in ["/cat"]:
            self.func_DO()
        else:
            self.send_response(400)
            text="<h1 align=center>Bad request</h1>"
            self.func_PRINT(text)
    def func_PARSE(self):
        """
        Parse a URL into six components
        """
        self.parsed_url = parse.urlparse("http://{0}:{1}{2}".format(args.HTTP_HOST, args.HTTP_PORT, self.path).lower())
        self.parsed_param = parse.parse_qs(self.parsed_url[4])
    def func_DO(self):
        """
        Do something by getting a specific request
        """
        if self.command == "GET" and self.parsed_url[2] == "/ping":
            text="Cats Service. Version 0.1"
            self.send_response(200)
            self.func_PRINT(text)
            return
        if self.command == "GET" and self.parsed_url[2] == "/cats":
            order=""
            limit=""
            offset=""
            if self.parsed_param.keys():
                for i in self.parsed_param.keys():
                    if i in ["attribute", "order", "offset", "limit"]:
                        continue
                    else:
                        text="Bad URL parameters"
                        self.send_response(400)
                        self.func_PRINT(text)
                        return
                if self.parsed_param.get("attribute"):
                    if self.parsed_param["attribute"][0] in ["name", "color", "tail_length", "whiskers_length"]:
                        order="ORDER BY {0} {1}".format(self.parsed_param["attribute"][0], self.parsed_param.get("order")[0].upper())
                    else:
                        text="Bad attribute"
                        self.send_response(400)
                        self.func_PRINT(text)
                        return
                if self.parsed_param.get("order"):
                    if self.parsed_param["order"][0] in ["asc", "desc"]:
                        pass
                    else:
                        text="Bad order value"
                        self.send_response(400)
                        self.func_PRINT(text)
                        return
                if self.parsed_param.get("limit"):
                    try:
                        if int(self.parsed_param["limit"][0]) >= 0:
                            limit="LIMIT {0}".format(int(self.parsed_param["limit"][0]))
                        else:
                            text="Limit is negative"
                            self.send_response(400)
                            self.func_PRINT(text)
                            return
                    except ValueError:
                        text="Bad limit value"
                        self.send_response(400)
                        self.func_PRINT(text)
                        return
                if self.parsed_param.get("offset"):
                    try:
                        if int(self.parsed_param["offset"][0]) >= 0:
                            offset="OFFSET {0}".format(int(self.parsed_param["offset"][0]))
                        else:
                            text="Offset is negative"
                            self.send_response(400)
                            self.func_PRINT(text)
                            return
                    except ValueError:
                         text="Bad offset value"
                         self.send_response(400)
                         self.func_PRINT(text)
                         return
            with connection.cursor(cursor_factory=ex.DictCursor) as cursor:
                cursor.execute("SELECT row_to_json(t) as cats FROM (select name, color, tail_length, whiskers_length from public.cats {0} {1} {2} ) t".format(order, offset, limit))
                result=cursor.fetchall()
                text={"cats":[row[0] for row in result]}
            self.send_response(200)
            self.func_PRINT(json.dumps(text, indent=4, separators=(",", ":")))
            return
        if self.command == "POST" and self.parsed_url[2] == "/cat":
            color="NULL"
            t_length="NULL"
            w_length="NULL"
            try:
                content_length = int(self.headers.get('Content-Length'))
                post_body = json.loads(self.rfile.read(content_length))
                self.send_response(200)
            except ValueError:
                text = "JSON object is invalid"
                self.send_response(400)
                self.func_PRINT(text)
                return
            except TypeError:
                text = "POST body is empty"
                self.send_response(400)
                self.func_PRINT(text)
                return
            for i in post_body.keys():
                if i.lower() == "name":
                    break
                else:
                    continue
            else:
                text="Name is not exists"
                self.send_response(400)
                self.func_PRINT(text)
                return
            for i in post_body.keys():
                if i.lower() in ["name", "color", "tail_length", "whiskers_length"]:
                    if i.lower() == "name":
                        if not type(post_body[i]) == str or post_body[i].isspace():
                            text="Invalid name"
                            self.send_response(400)
                            self.func_PRINT(text)
                            return
                        with connection.cursor(cursor_factory=ex.DictCursor) as cursor:
                            cursor.execute("SELECT LOWER(t.name) FROM public.cats t WHERE LOWER(t.name) = LOWER('{0}')".format(post_body[i]))
                            name=cursor.fetchall()
                        if name:
                            text = "Name alredy exists"
                            self.send_response(400)
                            self.func_PRINT(text)
                            return
                        else:
                            name="'{}'".format(post_body[i])
                    if i.lower() == "color":
                        if not type(post_body[i]) == str or post_body[i].isspace():
                            text = "Invalid color value"
                            self.send_response(400)
                            self.func_PRINT(text)
                            return
                        elif post_body[i].lower() not in ["black", "white", "black & white", "red", "red & black & white", "red & white"]:
                            text = "Invalid color value (must be 'black', 'white', 'red', 'black & white', 'red & black' or 'red & black & white')"
                            self.send_response(400)
                            self.func_PRINT(text)
                            return
                        else:
                            color="'{}'".format(post_body[i])
                    if i.lower() == "tail_length":
                        try:
                            if int(post_body[i]) >= 0:
                                t_length = post_body[i]
                            else:
                                text="Tail length is negative"
                                self.send_response(400)
                                self.func_PRINT(text)
                                return
                        except ValueError:
                            text="Bad tail length value"
                            self.send_response(400)
                            self.func_PRINT(text)
                            return
                    if i.lower() == "whiskers_length":
                        try:
                            if int(post_body[i]) >= 0:
                                w_length = post_body[i]
                            else:
                                text="Whiskers length is negative"
                                self.send_response(400)
                                self.func_PRINT(text)
                                return
                        except ValueError:
                            text="Bad whiskers length value"
                            self.send_response(400)
                            self.func_PRINT(text)
                            return
            with connection.cursor(cursor_factory=ex.DictCursor) as cursor:
                cursor.execute("INSERT INTO public.cats (name, color, tail_length, whiskers_length) VALUES ( {0}, {1}, {2}, {3})".format(name, color, t_length, w_length))
            text="Cat {0} added to the database".format(name)
            self.send_response(200)
            self.func_PRINT(text)
            return
    def func_PRINT(self, text):
        """
        Return GET/POST request result
        """
        self.send_header("Content-type","text/html")
        self.end_headers()
        self.wfile.write(bytes("<!DOCTYPE html><html><head><title>Cats Service</title></head><body><h1 align=center>RESULT</h1><p align=center>{0}</p></body></html>\n".format(text).encode()))

def task1():
    """
    Do task 1 (color statistics)
    """
    with connection.cursor(cursor_factory=ex.DictCursor) as cursor:
        cursor.execute("INSERT INTO public.cat_colors_info SELECT t.color, COUNT(*) FROM public.cats t GROUP BY t.color")
        cursor.execute("SELECT t.color, COUNT(*) FROM public.cats t WHERE t.color IS NOT NULL GROUP BY t.color")
        for row in cursor:
            print(row)
        print("RECORDED IN THE TABLE 'cat_colors_info'")

def task2():
    """
    Do task 2 (tail and whiskers length statistics)
    """
    with connection.cursor() as cursor:
        cursor.execute("SELECT t.tail_length FROM public.cats t WHERE t.tail_length IS NOT NULL")
        tl_list = [row[0] for row in cursor]
        cursor.execute("SELECT t.whiskers_length FROM public.cats t WHERE t.whiskers_length IS NOT NULL")
        wl_list = [row[0] for row in cursor]
        tl_mean = round(statistics.mean(tl_list), 1)
        wl_mean = round(statistics.mean(wl_list), 1)
        tl_median = round(statistics.median(tl_list), 1)
        wl_median = round(statistics.median(wl_list), 1)
        tl_mode=[]
        wl_mode=[]
        try:
            tl_mode.append(statistics.mode(tl_list))
            tl_mode=set(tl_mode)
        except statistics.StatisticsError:
            tl_repeat=max([tl_list.count(i) for i in set(tl_list)])
            tl_mode = set([i for i in tl_list if tl_list.count(i) == tl_repeat])
        try:
            wl_mode.append(statistics.mode(wl_list))
            wl_mode=set(wl_mode)
        except statistics.StatisticsError:
            wl_repeat=max([wl_list.count(i) for i in set(wl_list)])
            wl_mode = set([i for i in wl_list if wl_list.count(i) == wl_repeat])
        cursor.execute("INSERT INTO public.cats_stat (tail_length_mean, tail_length_median, tail_length_mode, whiskers_length_mean, whiskers_length_median, whiskers_length_mode) VALUES ( {0}, {1}, '{2}', {3}, {4}, '{5}' )".format(tl_mean, tl_median, str(tl_mode), wl_mean, wl_median, str(wl_mode)))
    print("tail_length_mean: {0}\ntail_length_median: {1}\ntail_length_mode: {2}\nwhiskers_length_mean: {3}\nwhiskers_length_median: {4}\nwhiskers_length_mode: {5}\n".format(tl_mean, tl_median, str(tl_mode), wl_mean, wl_median, str(wl_mode)))
    print("RECORDED IN THE TABLE 'cats_stat'")

def taskHTTP():
    """
    Start HTTP server
    """
    server = HTTPServer((args.HTTP_HOST, args.HTTP_PORT), ForgeHandler)
    print("HTTP server started\naddress: http://{0}:{1}".format(args.HTTP_HOST, args.HTTP_PORT))
    global st_point, cur_request
    cur_request=1
    st_point=time.time()
    while True:
        server.handle_request()
        #time.sleep(0.01)
        cur_request+=1

if __name__ == "__main__":
    while True:
        try:
            connection = psycopg2.connect(host=args.DB_HOST, port=args.DB_PORT, dbname=args.DB_NAME, user=args.DB_USER, password=args.DB_PASS)
            connection.autocommit=True
            print("Select any task number:\n\t1 -> task 1\t[cat colors info]\n\t2 -> task 2\t[cats stat]\n\t3 -> tasks 3-6\t[start HTTP server ('Ctrl-C' for stop server)]\n\t0 -> exit\t[press key '0' for exit]")
            number=input("number: ")
            if number == "1":
                task1()
            elif number == "2":
                task2()
            elif number == "3":
                taskHTTP()
            elif number == "0":
                exit(0)
            else:
                print("WRONG NUMBER")
        except psycopg2.OperationalError:
            print("Could not connect to server: Invalid arguments")
            exit(1)
        except KeyboardInterrupt:
            print("\nInterrupted by user")
            if number == "3":
                pass
            else:
                exit(0)
        except psycopg2.IntegrityError:
            print("DATA ALREADY EXIST")
        except OSError as Error:
            if Error.errno == 99:
                print("Cannot assign requested address: http://{0}:{1}".format(args.HTTP_HOST, args.HTTP_PORT))
                exit(1)
