def make():
    lons = [-57., -51.]
    lats = [1., 6.]
    delta_lon, delta_lat = .1, .1
    
    f = open('../data/grid.json', 'wt')
    f.write('{"type": "FeatureCollection", "features": [')
    
    this_lat, this_lon = lats[0], lons[0]
    i = 0
    done = False
    while not done:
        f.write('{"geometry": {"type": "Polygon", "coordinates": [[[' + str(this_lon) + ', ' + str(this_lat) + '], [' + str(this_lon) + ', ' + str(round(this_lat + delta_lat, 1)) + '], [' + str(round(this_lon + delta_lon, 1)) + ', ' + str(round(this_lat + delta_lat, 1)) + '], [' + str(round(this_lon + delta_lon, 1)) + ', ' + str(this_lat) + '], [' + str(this_lon) + ', ' + str(this_lat) + ']]]}, "type": "Feature", "properties": {"w": "' + str(delta_lat) + '", "value": "nan"}, "id": "' + str(i) + '"}')
        i += 1
        this_lon = round(this_lon + delta_lon, 1)
        if this_lon == lons[1]:
            this_lon = lons[0]
            this_lat = round(this_lat + delta_lat, 1)
            if this_lat == lats[1]:
                done = True
        if not done:
            f.write(', ')
    
    f.write(']}\n')
    f.close()
