VGA Forum Search

A crawler/webapp for the VGA forum site. 
Nothing special and you probably don't need to run this.

You need:
 * Python 2.7 or newer.
 * BeautifulSoup version 3.
 * Sqlite 3 with RTS extension.
 * Python Requests.
 * A WSGI supported web server. 

Usage: 
 1. Run a crawler.
    $ python crawl.py -n vgaforum.db http://videogamesawesome.com/forums/forum/minecraft/

 2. Copy vgaforum.db to the same directory with app.py

 3. Run app.py via WSGI.
