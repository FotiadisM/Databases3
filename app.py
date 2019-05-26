# ----- CONFIGURE YOUR EDITOR TO USE 4 SPACES PER TAB ----- #
import sys, os
sys.path.append(os.path.join(os.path.split(os.path.abspath(__file__))[0], 'lib'))
import pymysql as db
import settings


def connection():
    ''' User this function to create your connections '''
    con = db.connect(
        settings.mysql_host,
        settings.mysql_user,
        settings.mysql_passwd,
        settings.mysql_schema)

    return con


def classify(topn):

    # Create a new connection
    con = connection()
    # Create a cursor on the connection
    cur = con.cursor()

    sqlQuery = """
        SELECT
            title, summary
        FROM
            articles
        WHERE
            NOT id IN (
                SELECT
                    articles_id
                FROM
                    article_has_class
            )
    ;"""

    cur.execute(sqlQuery)
    data = cur.fetchall()

    result = [("title", "class", "subclass", "weightsum"), ]
    for row in data:
        result.append(row)

    return result


def updateweight(class1, subclass, weight):

   # Create a new connection
    con = connection()
    # Create a cursor on the connection
    cur = con.cursor()

    sqlQuery = """
        UPDATE
            classes
        SET
            classes.weight = classes.weight - (classes.weight - %d) / 2
        WHERE
            classes.class = "%s"
            AND classes.subclass = "%s"
            AND classes.weight > %d
    ;""" % ( int(weight), class1, subclass, int(weight))

    try:
        cur.execute(sqlQuery)
        con.commit()
        data = (("ok", ), )
    except:
        con.rollback()
        data = (("error", ), )

    cur.close()
    con.close()

    result = [("result", ), ]
    for row in data:
        result.append(row)

    return result

def selectTopNClasses(fromdate, todate,n):

    # Create a new connection
    con = connection()
    # Create a cursor on the connection
    cur = con.cursor()

    sqlQuery = """
        SELECT
            article_has_class.class, article_has_class.subclass, COUNT(DISTINCT article_has_class.articles_id)
        FROM
            article_has_class
        WHERE
            article_has_class.articles_id IN (
                SELECT
                    articles.id
                FROM
                    articles, article_has_class
                WHERE
                    articles.id = article_has_class.articles_id
                    AND articles.date > "%s"
                    AND articles.date < "%s"
            )
        GROUP BY
            article_has_class.class, article_has_class.subclass
        ORDER BY
            COUNT(DISTINCT article_has_class.articles_id) DESC
        LIMIT %d
    ;""" % ( fromdate, todate, int(n))

    cur.execute(sqlQuery)
    data = cur.fetchall()
    cur.close()
    con.close()

    result = [("class", "subclass", "count"), ]
    for row in data:
            result.append(row)

    return result

def countArticles(class1, subclass):

    # Create a new connection
    con = connection()
    # Create a cursor on the connection
    cur = con.cursor()

    sqlQuery = """
        SELECT
            COUNT( DISTINCT article_has_class.articles_id)
        FROM
            article_has_class
        WHERE
            article_has_class.articles_id IN (
            SELECT
                article_has_class.articles_id
            FROM
                article_has_class
            WHERE
                article_has_class.class = "%s"
                AND article_has_class.subclass = "%s"
        )
    ;""" % ( class1, subclass)

    cur.execute(sqlQuery)
    data = cur.fetchall()
    cur.close()
    con.close()

    result = [("count", ), ]
    for row in data:
        result.append(row)

    return result

def findSimilarArticles(articleId,n):

    # Create a new connection
    con = connection()

    # Create a cursor on the connection
    cur = con.cursor()

    cur.execute("")
    return [("articleid",), ]