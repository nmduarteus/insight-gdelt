from flask import Flask, render_template
from postgres_data import PostgresqlObj

app = Flask(__name__)

@app.route('/')
@app.route('/index')
def webui():
    cameo_codes = PostgresqlObj()
    cameo_categories = cameo_codes.get_all_categories()
    cameo_subcategories = cameo_codes.get_all_subcategories()
    countries = cameo_codes.get_countries_with_most_events()
    tone_per_month = cameo_codes.avg_tone_per_month()
    #types_across_time = cameo_codes.types_across_time()
    top_channels = cameo_codes.top_channels()
    top_words = cameo_codes.top_words()
    #print(top_channels)
    cameo_codes.close()

    data = {'categories': cameo_categories, 'subcategories': cameo_subcategories, 'countries': countries, 'tone_per_month' : tone_per_month, 'top_channels' : top_channels, 'top_words': top_words }
    #print(types_across_time)
    return render_template('index.html', title='GDELT+', data=data)

if __name__ == "__main__":
    app.run(port="80", host="0.0.0.0",debug=True)