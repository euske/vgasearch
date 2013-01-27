#!/usr/bin/env python
##
##  A WordPress forum crawler.
##  usage: python crawl.py out.db url
##

import sys, re, time, array
import requests
import urlparse
import sqlite3
from BeautifulSoup import BeautifulSoup


DATE = re.compile(r'(\w+)\s+(\d+)\s*,\s*(\d+)\s+at\s+(\d+):(\d+)\s+(\w+)', re.I)
MONTH = ('january','february','march','april','may','june',
         'july','august','september','october','november','december')
def getdate(x):
    m = DATE.match(x.strip())
    (month,day,year,hh,mm,ampm) = m.groups()
    month = MONTH.index(month.lower())+1
    day = int(day)
    year = int(year)
    hh = int(hh)
    mm = int(mm)
    if ampm.lower() == 'pm':
        hh += 12
    return int(time.mktime((year,month,day,hh,mm,0,0,0,0)))

def getcls(cls, elems):
    for x in elems:
        k = x.get('class')
        if k and cls in k:
            yield x
    return

# unentify(x): decodes HTML entities to original text.
UNENTIFY = re.compile(ur'&([^;]+);')
def unentify(x):
    from htmlentitydefs import name2codepoint
    def f(m):
        k = m.group(1)
        if k.startswith('#x'):
            try:
                c = int(k[2:], 16)
            except ValueError:
                return u''
        elif k.startswith('#'):
            try:
                c = int(k[1:])
            except ValueError:
                return u''
        else:
            try:
                c = name2codepoint[k]
            except KeyError:
                return k
        return unichr(c)
    return UNENTIFY.sub(f, x)


##  Crawler
##
class Crawler(object):

    def __init__(self, conn):
        self.conn = conn
        self.session = requests.session()
        return

    def close(self):
        self.session.close()
        return

    def fetch(self, url):
        #print >>sys.stderr, 'fetching: %r' % url
        resp = self.session.get(url)
        if resp.status_code != 200: return None
        soup = BeautifulSoup(resp.content)
        return soup

    def getnpages(self, url):
        print >>sys.stderr, 'getnpages: %r' % url
        soup = self.fetch(url)
        n = 1
        for x in getcls('bbp-pagination-links', soup.findAll('div')):
            for y in getcls('page-numbers', x.findAll('a')):
                try:
                    n = max(n, int(y.text))
                except ValueError:
                    pass
        return n

    def getthreads(self, url):
        print >>sys.stderr, 'getthread: %r' % url
        soup = self.fetch(url)
        for x in getcls('bbp-topic-permalink', soup.findAll('a')):
            yield (x['href'], unentify(x.text))
        return

    def getposts(self, url):
        print >>sys.stderr, 'getposts: %r' % url
        soup = self.fetch(url)
        date = None
        for x in soup.findAll('div'):
            k = x.get('class')
            if not k: continue
            if ('bbp-meta' in k):
                spans = x.findAll('span')
                date = getdate(spans[0].text)
            elif ('topic' in k) or ('reply' in k):
                try:
                    if not x['id'].startswith('post-'): continue
                except KeyError:
                    continue
                pid = int(x['id'][5:])
                divs = x.findAll('div', recursive=False)
                if len(divs) < 2: continue
                if 'bbp-reply-author' not in divs[0].get('class'): continue
                username = None
                for y in getcls('bbp-author-name', divs[0].findAll('a')):
                    username = unentify(y.text)
                if 'bbp-reply-content' not in divs[1].get('class'): continue
                text = unentify(divs[1].text)
                yield (pid, date, username, text)
        return

    def run(self, url0):
        print >>sys.stderr, 'run: %r' % url0
        cur = self.conn.cursor()
        npages = self.getnpages(url0)
        for page in xrange(1,npages+1):
            url1 = urlparse.urljoin(url0, 'page/%d' % page)
            for (url2,title) in self.getthreads(url1):
                #print (url2,title)
                tid = None
                posts = array.array('I')
                npages = self.getnpages(url2)
                for page in xrange(1,npages+1):
                    url3 = urlparse.urljoin(url2, 'page/%d' % page)
                    for (pid,date,username,text) in self.getposts(url3):
                        #print (pid, username, text)
                        if tid is None:
                            tid = pid
                            content = title+'\n'+username+'\n'+text
                        else:
                            content = username+'\n'+text
                        cur.execute('SELECT pid FROM post WHERE pid = ?;', (pid,))
                        pids = cur.fetchall()
                        if pids:
                            print >>sys.stderr, ' overlap: pid %r' % (pid)
                            continue
                        if cur.fetchone(): continue
                        cur.execute('INSERT INTO content VALUES (?);', (content,))
                        docid = cur.lastrowid
                        cur.execute('INSERT INTO post VALUES (?,?,?,?,?,?);',
                                    (pid,tid,page,date,username,docid))
                        cur.execute('INSERT INTO doc VALUES (?,?);', (docid,pid))
                        posts.append(pid)
                cur.execute('SELECT tid FROM topic WHERE tid = ?;', (tid,))
                tids = cur.fetchall()
                if tids:
                    print >>sys.stderr, ' overlap: tid %r' % (tid)
                    continue
                cur.execute('INSERT INTO topic VALUES (?,?,?,?);',
                            (tid,title,url2,buffer(posts.tostring())))
                print >>sys.stderr, ' added %s posts' % len(posts)
                sys.stderr.flush()
                self.conn.commit()
        return

def main(argv):
    import getopt
    import os
    def usage():
        print 'usage: %s [-n] out.db url ...' % argv[0]
        return 100
    try:
        (opts, args) = getopt.getopt(argv[1:], 'nd')
    except getopt.GetoptError:
        return usage()
    debug = 0
    create = False
    for (k, v) in opts:
        if k == '-d': debug += 1
        elif k == '-n': create = True
    if not args: return usage()
    dbpath = args.pop(0)
    if create:
        try:
            os.remove(dbpath)
        except OSError:
            pass
    conn = sqlite3.connect(dbpath)
    if create:
        cur = conn.cursor()
        cur.execute('CREATE VIRTUAL TABLE content USING fts3(text TEXT);')
        cur.execute('CREATE TABLE doc '
                    '(docid INTEGER PRIMARY KEY, pid INTEGER);')
        cur.execute('CREATE TABLE post '
                    '(pid INTEGER PRIMARY KEY, tid INTEGER,'
                    ' page INTEGER, date INTEGER, username TEXT, docid INTEGER);')
        cur.execute('CREATE TABLE topic '
                    '(tid INTEGER PRIMARY KEY, title TEXT, url TEXT, pids BLOB);')
    crawler = Crawler(conn)
    for url in args:
        crawler.run(url)
    crawler.close()
    conn.close()
    return
    
if __name__ == '__main__': sys.exit(main(sys.argv))
