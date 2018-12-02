from bottle import Bottle, run, view, static_file, response
import click
 
app = Bottle()

global language, param, rivers, this_host

language = 'fr' # by default, language is set to French ('fr' for French, 'en' for English)

# 'param' and 'rivers' are used by the template (index.tpl) to generate an HTML page in French or English

param = {'fr': {'language': 'English', 'title0': 'Pluies satellite (mm/jour)', 'title1': 'Débits simulés (mm/jour)', 'download_pq_1d': 'Télécharger les pluies et les débits de bassin (mm/jour)', 'download_pq_1m': 'Télécharger les pluies et les débits de bassin (mm/mois)', 'download_pq_1y': 'Télécharger les pluies et les débits de bassin (mm/an)', 'download_p2d_1d': 'Télécharger les pluies spatialisées (mm/jour, 6N-1N/57W-51W)'}, 'en': {'language': 'Français', 'title0': 'Satellite-based rainfall estimates (mm/day)', 'title1': 'Simulation-based streamflow estimates (mm/day)', 'download_pq_1d': 'Download catchment rainfall and streamflow (mm/day)', 'download_pq_1m': 'Download catchment rainfall and streamflow (mm/month)', 'download_pq_1y': 'Download catchment rainfall and streamflow (mm/year)', 'download_p2d_1d': 'Download spatial rainfall (mm/day, 6N-1N/57W-51W)'}}

rivers = {'fr': [["La Mana à Saut Sabbat", "q_saut_sabbat.json"], ["La Comté à Saut Bief", "q_saut_bief.json"], ["L'Approuague à Saut Athanase", "q_saut_athanase.json"], ["L'Oyapock à Saut Maripa", "q_saut_maripa.json"], ["La Lawa à Maripasoula", "q_maripasoula.json"], ["Le Maroni à Langa Tabiki", "q_langa_tabiki.json"]], 'en': [["Mana River at Saut Sabbat", "q_saut_sabbat.json"], ["Comté River at Saut Bief", "q_saut_bief.json"], ["Approuague River at Saut Athanase", "q_saut_athanase.json"], ["Oyapock River at Saut Maripa", "q_saut_maripa.json"], ["Lawa River at Maripasoula", "q_maripasoula.json"], ["Maroni River at Langa Tabiki", "q_langa_tabiki.json"]]}

this_host = 'http://guyane.irstea.fr' # URL of the host (can be 'http://...' or '' if local host)

@app.route('/static/<filename>', name='static')
def server_static(filename):
    """Route to static files."""
    resp = static_file(filename, root='static')
    resp.set_header('Cache-Control', 'no-cache')
    return resp

@app.route('/data/<filename>', name='data')
def server_data(filename):
    """Route to data files."""
    resp = static_file(filename, root='data')
    resp.set_header('Cache-Control', 'no-cache')
    return resp

@app.route('/')
@view('views/index.tpl')
def index():
    """Route to the index."""
    global language, param
    this_language = language
    if language == 'fr':
        language = 'en'
    else:
        language = 'fr'
    return {'param': param[this_language], 'rivers': rivers[this_language], 'this_host': this_host}

@click.command()
@click.option('--ip', default='localhost', help='IP address the web server listens to.')
@click.option('--port', default='8080', help='Port number the web server listens to.')
def main(ip, port):
    run(app, host=ip, port=port)

if __name__ == '__main__':
    main()
