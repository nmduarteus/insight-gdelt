import queries
from flask import Flask, render_template
from postgres_data import PostgresqlObj

app = Flask(__name__)


@app.route('/')
@app.route('/index')
def webui():
    db_data = PostgresqlObj()

    cameo_categories = db_data.execQuery(queries.get_all_categories)
    cameo_subcategories = db_data.execQuery(queries.get_all_subcategories)
    countries = db_data.execQuery(queries.get_countries_with_most_events).toJson()
    tone_per_month = db_data.execQuery(queries.avg_tone_per_month)
    top_channels = db_data.execQuery(queries.top_channels)
    top_words = db_data.execQuery(queries.top_words)
    db_data.close()

    # data to be passed to front end
    data = {'categories': cameo_categories, 'subcategories': cameo_subcategories, 'countries': countries, 'tone_per_month': tone_per_month, 'top_channels': top_channels, 'top_words': top_words}

    #return top_words
    return render_template('index.html', title='GDELT+', data=data)


if __name__ == "__main__":
    app.run(port="80", host="0.0.0.0", debug=True)
