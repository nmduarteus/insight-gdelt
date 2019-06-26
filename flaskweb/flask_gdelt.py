from flask import Flask, render_template
from postgres_data import PostgresqlObj

app = Flask(__name__)

@app.route('/')
@app.route('/index')
def webui():
    cameo_codes = PostgresqlObj()
    cameo_categories = cameo_codes.get_all_categories()
    cameo_subcategories = cameo_codes.get_all_subcategories()
    cameo_codes.close()

    data = {'categories': cameo_categories, 'subcategories': cameo_subcategories}
    return render_template('index.html', title='GDELT+', data=data)
