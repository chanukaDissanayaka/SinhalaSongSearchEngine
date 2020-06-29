import flask
from flask import Flask, render_template, request
from Searchengine import Search
from Searchengine import prepareQueries


app = Flask(__name__)
app.config["DEBUG"] = True


@app.route('/', methods=['GET'])
def home():
    return render_template("index.html")


@app.route('/search', methods=['POST', 'GET'])
def search():
    if request.method == 'POST':
        form = request.form

        res = Search.search(form['Query'])
        count = len(res)

        return render_template('result.html', results = res, count = count)


app.run()
