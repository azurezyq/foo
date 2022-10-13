from io import StringIO
import os

from flask import Flask
from flask import render_template
import sqlalchemy
import tabulate

db_user = os.environ.get('SQL_USERNAME', '')
db_password = os.environ.get('SQL_PASSWORD', '')
db_port = int(os.environ.get('SQL_PORT', 4000))
db_address = os.environ.get('SQL_ADDRESS', '')
db_name = os.environ.get('SQL_DB_NAME', '')

app = Flask(__name__)

engine_url = 'mysql+pymysql://{}:{}@{}:{}/{}'.format(
    db_user, db_password, db_address, db_port, db_name)
engine = sqlalchemy.create_engine(engine_url, pool_size=3)

LEADER_STMT = '''
SELECT t.author, COUNT(*), SUM(t.additions), SUM(t.deletions)
FROM 
(SELECT author, additions, deletions FROM pr ORDER BY insertTime DESC LIMIT 100) AS t
GROUP BY t.author
ORDER BY COUNT(*) DESC, t.author ASC
LIMIT 20
;
'''

STREAM_STMT = '''
SELECT
  insertTime, author, owner, repo, additions, deletions, url, title
FROM pr ORDER BY insertTime DESC LIMIT 20;
'''

PAGE = '''
<html>
  <head>
    <script src="https://ajax.googleapis.com/ajax/libs/jquery/3.6.0/jquery.min.js"></script>
  </head>
  <div id=history>
  </div>
  <script language=javascript>
$(document).ready(function()
{
    function refresh()
    {
				$.ajax({url: "/demo", success: function(result){
							$("#history").html(result);
						}});
    }

    setInterval(function()
    {
        refresh()
    }, 1000); // 1s
})
  </script>
</html>
'''

def RenderLeaders(cnx, output):
  cursor = cnx.execute(LEADER_STMT)
  result = cursor.fetchall()
  out_rows = []
  for row in result:
    author, cnt, additions, deletions = row
    out_rows.append((author, cnt, f'+{additions}/-{deletions}'))
  print(tabulate.tabulate(out_rows), file=output)

def RenderList(cnx, output):
  cursor = cnx.execute(STREAM_STMT)
  result = cursor.fetchall()
  out_rows = []
  for row in result:
    insertTime, author, owner, repo, additions, deletions, url, title = row
    out_rows.append((insertTime, author, f'{owner}/{repo}', f'+{additions}/-{deletions}', url, title))
  print(tabulate.tabulate(out_rows), file=output)

@app.route('/')
def root():
  return PAGE


@app.route('/demo')
def demo():
  cnx = engine.connect()
  sio = StringIO()
  print('<pre>', file=sio)
  RenderLeaders(cnx, sio)
  RenderList(cnx, sio)
  cnx.close()
  print('</pre>', file=sio)
  return sio.getvalue()


if __name__ == '__main__':
  app.run(host='127.0.0.1', port=8080, debug=True)
