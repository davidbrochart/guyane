<!DOCTYPE html>
<meta charset="utf-8">
<html>
  <head>
    <style>
      body{
      
      }
      /* CSS goes here. */
      .subunit.GUI { fill: #dcd; }

      .subunit-boundary {
      fill: none;
      stroke: #777;
      stroke-dasharray: 2,2;
      stroke-linejoin: round;
      }

      .cb-text{
      color: #333333;
      }

      .map-title{
      color: #333333;
      font-size: 20px;
      }

      .domain{
      stroke: none;
      fill: none;
      }

      #maps-holder{
      font-family: Helvetica, sans;
      font-size: 12px;
      }
      
      h3#subtitle{
      text-align: center;
      font-weight: bold;
      }
    </style>
    <body>
      <link rel="stylesheet" type="text/css" href="trmm/static/main.css">
      <link rel="stylesheet" href="trmm/static/d3.slider.css" />
      <script src="trmm/static/jquery-1.8.3.js"></script>
      <link href="//netdna.bootstrapcdn.com/bootstrap/3.0.0/css/bootstrap.min.css" rel="stylesheet">
      <script src="//netdna.bootstrapcdn.com/bootstrap/3.0.0/js/bootstrap.min.js"></script>
      <script src="trmm/static/d3.min.js"></script>
      <script src="trmm/static/cubism.v1.min.js"></script>
      <script src="trmm/static/topojson.v1.min.js"></script>
      <script src="trmm/static/d3.slider.js"></script>
      <script src="trmm/static/underscore-min.js"></script>
      <script type="text/javascript" src="trmm/static/iframeResizer.contentWindow.min.js"></script>
      <script src="trmm/static/minpubsub.js"></script>

      <input type="hidden" id="river0" value="{{rivers[0][0]}}" />
      <input type="hidden" id="river1" value="{{rivers[1][0]}}" />
      <input type="hidden" id="river2" value="{{rivers[2][0]}}" />
      <input type="hidden" id="river3" value="{{rivers[3][0]}}" />
      <input type="hidden" id="river4" value="{{rivers[4][0]}}" />
      <input type="hidden" id="river5" value="{{rivers[5][0]}}" />
      <input type="hidden" id="river0File" value="{{rivers[0][1]}}" />
      <input type="hidden" id="river1File" value="{{rivers[1][1]}}" />
      <input type="hidden" id="river2File" value="{{rivers[2][1]}}" />
      <input type="hidden" id="river3File" value="{{rivers[3][1]}}" />
      <input type="hidden" id="river4File" value="{{rivers[4][1]}}" />
      <input type="hidden" id="river5File" value="{{rivers[5][1]}}" />

      <div class='container'>
          <a href="{{this_host}}/trmm">TRMM</a>
          <a href="{{this_host}}">GPM</a>
          <a href="{{this_host}}">{{param['language']}}</a>
        <h2 id='main-title'>{{param['title0']}}</h2>
        <h3 id='subtitle'></h3>
        <button type="checkbox" id="mapAnim">Animation</button>
        <div class='row' id='maps-holder'>
          <div id='vizsun' class='col-md-4'></div>
        </div>
        <h2 id='main-title2'>{{param['title1']}}</h2>
        <div class='row' id='ts-holder'>
          <div id='timeseries'</div>
        </div>
      </div>

      <a href="{{this_host}}/trmm/data/pq_1d.csv">{{param['download_pq_1d']}}</a>
      <br>
      <a href="{{this_host}}/trmm/data/pq_1m.csv">{{param['download_pq_1m']}}</a>
      <br>
      <a href="{{this_host}}/trmm/data/pq_1y.csv">{{param['download_pq_1y']}}</a>
      <br>
      <a href="{{this_host}}/trmm/data/p2d_1d.zip">{{param['download_p2d_1d']}}</a>
      <br>

      <script src="trmm/static/rainfall.js"></script>

      <script src="trmm/static/streamflow.js"></script>

    </body>
    
  </head>

</html>
