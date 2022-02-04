from .markers import marker_ttm_sqz, marker_bb_overlower_2sig, marker_bb_overupper_2sig
# import mplfinance as mpf
from plotly import graph_objects as go 
from plotly.subplots import make_subplots
import plotly.express as px

# def mpl_stock_charts(data):

#     """
#     Function: 
#     This function is used to Plot MPLFinance plots for screened Stocks
#     """
#     # Code to plot subplots

#     ttm_signal = marker_ttm_sqz(data['SQZ_ON'], data['Low'])

#     bbu_over_2signal = marker_bb_overupper_2sig(data['BBU_21_2.0'], data['High'])

#     bbl_over_2signal = marker_bb_overlower_2sig(data['BBL_21_2.0'], data['Low'])

#     add_plot = [mpf.make_addplot(data['BBU_21_2.0'],color='b', panel=0),  # uses panel 0 by default
#             mpf.make_addplot(data['BBL_21_2.0'],color='b', panel=0),  # uses panel 0 by default
#             mpf.make_addplot(data['BBM_21_2.0'],color='cyan', panel=0),
#             mpf.make_addplot(data['KCLe_21_1.5'],color='magenta', panel=0),
#             mpf.make_addplot(data['KCUe_21_1.5'],color='magenta', panel=0),
#             mpf.make_addplot(data['ADX_21'],color='orange', panel=2, ylabel='ADX'),
#             mpf.make_addplot(ttm_signal,type='scatter', markersize=50,marker='.', color='b'),
#             mpf.make_addplot(bbu_over_signal, type='scatter', markersize=50,marker='v'),
#             mpf.make_addplot(bbl_over_signal, type='scatter', markersize=50,marker='^')
#           ]

#     mpf.plot(data,type='candle', volume=True,show_nontrading=False, style='yahoo',figratio=(10,8),figscale=2,
#             title = data['SYMBOL'].unique()[0], tight_layout=True,
#             scale_width_adjustment=dict(volume=0.7,candle=1.35),
#             update_width_config=dict(candle_linewidth=0.8),
#             main_panel = 0 , volume_panel=1, panel_ratios=(6,1),
#             addplot=add_plot,
#             hlines=dict(hlines=[data['High'].max(),data['Low'].min()],colors=['g','r'], linestyle='-.')#, linewidths = 0.2)
#             )

def plotly_stock_charts(data):
    
    """
    Function: To code plotly charts for Stocks
    """

    # ttm_signal = marker_ttm_sqz(data['SQZ_ON'], data['Low'])

    bbu_over_2signal = marker_bb_overupper_2sig(data['BBU_21_2.0'], data['High'])

    bbl_over_2signal = marker_bb_overlower_2sig(data['BBL_21_2.0'], data['Low'])

    buy_color = '#258F68'
    sell_color = '#D24130'

    fig = make_subplots(rows=3, cols=1, shared_xaxes=True, 
               vertical_spacing=0.03, subplot_titles=(None, 'Volume', 'ADX'), 
                row_heights = [0.8, 0.1, 0.1 ])

    colors = [sell_color if row['Open'] - row['Close'] >= 0 else buy_color for index, row in data.iterrows()]

    trace_candle=go.Candlestick(x=data['DATE'], open=data['Open'], high=data['High'], low=data['Low'], close=data['Close'],  yaxis = 'y2',
                              opacity = 1, line = dict(width=1), whiskerwidth = 0, name = str(data['SYMBOL'].unique()[0]),
                              increasing = dict( fillcolor = buy_color, line = dict( color = buy_color, width=1 ) ),
                              decreasing = dict(  fillcolor = sell_color, line = dict( color = sell_color, width=1 ) )
                )

    trace_ema = go.Scatter(x= data['DATE'], y=data['BBM_21_2.0'], mode = 'lines', yaxis = 'y2', name='EMA(21)', opacity=1,
                            marker = dict(color = 'black'), line = dict(color = 'black', width = 1), hoverinfo = 'skip')

    trace_bbu2sig = go.Scatter(x= data['DATE'], y=data['BBU_21_2.0'], mode = 'lines', yaxis = 'y2', name='BBU(21,2)', legendgroup='Bollinger Bands',
                            marker = dict(color = 'blue'), line = dict(width = 1), hoverinfo = 'skip')

    trace_bbl2sig = go.Scatter(x= data['DATE'], y=data['BBL_21_2.0'], mode = 'lines', yaxis = 'y2', name='BBL(21,2)', legendgroup='Bollinger Bands',
                            marker = dict(color = 'blue'), line = dict(width = 1), showlegend = False, hoverinfo = 'skip')
    
    trace_kcu= go.Scatter(x= data['DATE'], y=data['KCUe_21_1.5'], mode = 'lines', yaxis = 'y2', name='KCU(21,1.5)', legendgroup='Keltner Channel',
                            marker = dict(color = 'magenta'), line = dict(width = 1), hoverinfo = 'skip')

    trace_kcl = go.Scatter(x= data['DATE'], y=data['KCLe_21_1.5'], mode = 'lines', yaxis = 'y2', name= 'KCL(21,1.5)', legendgroup='Keltner Channel',
                            marker = dict(color = 'magenta'), line = dict(width = 1),  showlegend = False, hoverinfo = 'skip')

    trace_adx = go.Scatter(x= data['DATE'], y=data['ADX_21'], mode = 'lines', yaxis = 'y2', name='ADX(21)', opacity=1,
                            marker = dict(color = 'black'), line = dict(color = 'black', width = 1.5), hoverinfo = 'skip')

    trace_dmp = go.Scatter(x= data['DATE'], y=data['DMP_21'], mode = 'lines', yaxis = 'y2', name='DMP(21)', opacity=1,
                            marker = dict(color = 'green'), line = dict(color = 'green', width = 1),  hoverinfo = 'skip')
    
    trace_dmn = go.Scatter(x= data['DATE'], y=data['DMN_21'], mode = 'lines', yaxis = 'y2', name='DMN(21)', opacity=1,
                            marker = dict(color = 'red'), line = dict(color = 'red', width = 1), hoverinfo = 'skip')

    trace_vol = go.Bar(x= data['DATE'], y=data['Volume'], yaxis= 'y', name = 'Volume', marker=dict( color= colors ), hoverinfo = 'skip')

    trace_vol_avg = go.Scatter(x= data['DATE'], y=data['VOL_AVG_21'], mode = 'lines', yaxis = 'y2', name='VOL_AVG(21)', opacity=1,
                            marker = dict(color = 'blue'), line = dict(color = 'blue', width = 1), hoverinfo = 'skip')

    trace_bbu2sig_cross = go.Scatter(x= data['DATE'], y=bbu_over_2signal, mode = 'markers', marker_symbol=6,
                            marker_color = 'blue', marker_size = 6, line = dict(width = 1), showlegend = False, hoverinfo = 'skip')
    
    trace_bblsig_cross = go.Scatter(x= data['DATE'], y=bbl_over_2signal, mode = 'markers', marker_symbol=5,
                            marker_color = 'orange', marker_size = 6, line = dict(width = 1), showlegend = False, hoverinfo = 'skip')

    fig.add_traces(data = [trace_candle, trace_ema, trace_bbu2sig, trace_bbl2sig, trace_kcu, trace_kcl, trace_bbu2sig_cross, trace_bblsig_cross], 
                        rows=1, cols=1)
    
    fig.add_shape(type = "line", x0= data['DATE'].min(), y0=data['High'].max(), x1= data['DATE'].max(),  y1=data['High'].max(),  xref="x", yref="y",
                  line=dict(color="green", width=1, dash = 'dashdot'))

    fig.add_shape(type = "line", x0= data['DATE'].min(), y0=(data['High'].max() + data['Low'].min())/2, x1= data['DATE'].max(),  y1=(data['High'].max() + data['Low'].min())/2,  xref="x", yref="y",
                  line=dict(color="grey", width=0.8, dash = 'dashdot'))

    fig.add_shape(type = "line", x0= data['DATE'].min(), y0=data['Low'].min(), x1= data['DATE'].max(),  y1=data['Low'].min(),  xref="x", yref="y",
                  line=dict(color="red", width=1, dash = 'dashdot'))

    fig.add_traces(data = [trace_vol, trace_vol_avg], rows=2, cols=1)

    fig.add_traces(data = [trace_adx, trace_dmp, trace_dmn], rows=3, cols=1)


    fig.update_yaxes(showgrid=True, zeroline=False, showticklabels=True,
                 showspikes=True, spikemode='across', spikesnap='cursor', showline=False, spikedash='solid', spikecolor="grey",spikethickness=1)

    fig.update_xaxes(showgrid=True, zeroline=False, rangeslider_visible=False, 
                showspikes=True, spikemode='across', spikesnap='cursor', showline=False, spikedash='solid', spikecolor="grey",spikethickness=1,
                rangebreaks=[dict(bounds=["sat", "mon"])], 
                rangeselector=dict(buttons=list([
                dict(count=6, label="6m", step="month", stepmode="backward"),
                dict(count=9, label="9m", step="month", stepmode="backward"),
                dict(count=1, label="YTD",step="year", stepmode="todate"),
                dict(count=1, label="1y", step="year", stepmode="backward"),
                dict(step="all")
            ]))
      )

    fig.update_layout(title = str(data['SYMBOL'].unique()[0]), title_x=0.5, autosize=False, width=1450,height=800, legend_orientation='h',
                    paper_bgcolor="LightSteelBlue", margin=dict(l=30, r=30, t=30, b=20), showlegend=True,
                    hoverdistance=1, dragmode='pan', hovermode='closest'
                    )
# hoverdistance=0 - Blocks the hover msg.
    return (fig)



def plotly_index_chart(data):

      """
      Function: To code plotly charts for Indexes
      """
      fig = px.line(data, data['DATE'], data['CLOSE_SCALED'], color= data['SYMBOL'], 
                        hover_data =  {'CLOSE_SCALED' : False, 'CLOSE': True, 'DATE': False}  ,      
                        width = 1400, height = 700, title = 'INDIA INDEX ANALYSIS'
                        # facet_col = data['SYMBOL'],  facet_col_wrap = 3, #Property to split graph in multiple subgraphs
                        )
      fig.update_yaxes(showgrid=True, zeroline=False, showticklabels=True,  showspikes=True, spikemode='across', 
                      spikesnap='cursor', showline=False, spikedash='solid', spikecolor="grey",spikethickness=1)

      fig.update_xaxes(showgrid=True, zeroline=False, rangeslider_visible=False, showspikes=True, spikemode='across', 
                      spikesnap='cursor', showline=False, spikedash='solid', spikecolor="grey",spikethickness=1,
                      rangeselector=dict(buttons=list([
                      dict(count=6, label="6m", step="month", stepmode="backward"),
                      dict(count=1, label="1y", step="year", stepmode="backward"),
                      dict(count=5, label="5y", step="year", stepmode="backward"),
                      dict(count=10, label="10y", step="year", stepmode="backward"),
                      dict(count=1, label="YTD",step="year", stepmode="todate"),
                      dict(step="all")]))
          )

      fig.update_layout(title_x=0.5, autosize=False, legend_orientation='h',
              paper_bgcolor="LightSteelBlue", margin=dict(l=30, r=30, t=30, b=10), showlegend=True,
              hoverdistance=1, dragmode='pan', hovermode='x unified'
              )
      
      return (fig)


def tview_chart_widget(symbol_list):
    
        chart_widget = """ 
        <!-- TradingView Widget BEGIN -->
        <div class="tradingview-widget-container">
        <div id="tradingview_84a4d"></div>
        <div class="tradingview-widget-copyright"><a href="https://www.tradingview.com/symbols/BSE-TCS/" rel="noopener" target="_blank"><span class="blue-text">Chart</span></a> by TradingView</div>
        <script type="text/javascript" src="https://s3.tradingview.com/tv.js"></script>
        <script type="text/javascript">
        new TradingView.widget(
        {
        "width": 1400,
        "height": 800,
        "symbol": "BSE:TCS",
        "interval": "D",
        "timezone": "Asia/Kolkata",
        "theme": "light",
        "style": "1",
        "locale": "en",
        "toolbar_bg": "#f1f3f6",
        "enable_publishing": false,
        "withdateranges": true,
        "range": "ALL",
        "hide_side_toolbar": false,
        "allow_symbol_change": true,
        "details": true,
        "hotlist": true,
        "calendar": true,
        "show_popup_button": true,
        "popup_width": "1000",
        "popup_height": "650",
        "container_id": "tradingview_84a4d",
        "studies": [
                    "BB@tv-basicstudies",
                    "KLTNR@tv-basicstudies"
                  ],
        "watchlist":""" +  str(symbol_list) +  """,}
        );
        </script>
        </div>
        """
        return (chart_widget)


# def sr_line():
#     pivot_high = df['High'].rolling(60,min_periods=20).max()
#     pivot_low = df['Low'].rolling(60,min_periods=20).max()
#     pivot_hl = pd.concat([pivot_high, pivot_low], axis=0)
#     pivot_hl = pd.Series(np.sort(pivot_hl[pivot_hl.map(pivot_hl.value_counts()) >= 5].unique()))
#     pivot_hl = pivot_hl.rolling(window=3).mean().dropna().tolist()

#     s =  np.mean(df['High'] - df['Low'])
#     return pivot_hl 

# pivot_hl = sr_line()


# {
#                     "BB@tv-basicstudies"
#                 },
#                 {
#                     "KLTNR@tv-basicstudies"
#                     inputs: {
#                                 Length: 3,
#                             }
#                 },
#                 {
#                     "MAExp@tv-basicstudies"
#                 }