from dash import Dash, html, dcc, callback, Output, Input
from plotly import express as px
import pandas as pd
import sqlalchemy as sq

app = Dash(__name__)

app.layout = html.Div([
    html.H1(children='Agriculture Data', style={'textAlign':'center'}),
    html.Button(children='Refresh Data', id='refresh', n_clicks=0, style={'width': '100%', 'background':'lightBlue', 'border': 'none', 'height': '30px', 'borderRadius': '5px'}),
    html.Div(children=dcc.Graph(id='graph-content', style={'height': '100%'}), style={'height': "90vh"}),
])

def get_chart_data():
    import os
    user_name = 'karol_talbot'
    password = os.environ.get('SqlPass')
    connection_string = f'mysql+mysqlconnector://{user_name}:{password}@datasciencedb.ucalgary.ca/{user_name}'

    engine = sq.create_engine(connection_string)
    data = pd.read_sql_table("agriculture", engine)
    engine.dispose()
    return data

@callback(
    Output('graph-content', 'figure'),
    Input('refresh', 'n_clicks')
)
def update_graph(n_clicks):
    dff = get_chart_data()
    dff.sort_values(by='Year', ascending=True, inplace=True)
    return px.line(dff, x='Year', y='Value', color='Product')

if __name__ == '__main__':
    app.run(debug=False)