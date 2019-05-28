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
    categoryQuery = """
        SELECT
            DISTINCT class, subclass
        FROM
            classes
        LIMIT %d
    ;""" % ( int(topn))

    cur.execute(sqlQuery)
    data = cur.fetchall()
    cur.execute(categoryQuery)
    categoriesData = cur.fetchall()

    categories = []
    for row in categoriesData:
        info = [
            row[0].split(), # class
            row[1].split(), # subclass
            1,              # weight
            0               # weight count
        ]
        categories.append(info)

    articles = []
    for row in data:
        array = [
            row[0],         # title
            row[1].split(), # summary
            categories      # array of classes, subclasses, weight and the weight count
        ]
        articles.append(array)

    for article in articles:
        for word in article[1]:
            for category in article[2]:
                for classW in category[0]:
                    if word.lower() == classW.lower():
                        category[3] = category[3] + category[2]
                for subclassW in category[1]:
                    if word.lower() == subclassW.lower():
                        category[3] = category[3] + category[2]
            print("\n")

    for article in articles:
        print(article[0])
        for category in article[2]:
            print(category[3])
        print("\n")

    def findCategory(categories):
        THEcategory = categories[0]
        for category in categories:
            if category[3] > THEcategory[3]:
                THEcategory = category
        return THEcategory

    result = [("title", "class", "subclass", "weightsum"), ]
    for article in articles:
        THEcategory = findCategory(article[2])
        info = (
            article[0],
            " ".join(THEcategory[0]),
            " ".join(THEcategory[1]),
            THEcategory[3]
        )
        result.append(info)

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