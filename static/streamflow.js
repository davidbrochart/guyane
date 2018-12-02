(function(){
    var lastDate;
    var context;

function stock(name) { 
    return context.metric(function(start, stop, step, callback) {
        d3.json("data/" + name[1], function(rows) {
            var values = [];
            // Creates an array of the price differences throughout the day
            rows.forEach(function(d) {
                values.push(value = d);
            }); 
            callback(null, values); }); 
    }, name[0]); 
}


function draw_graph(stocks_list) {
    d3.select("#timeseries")                 // Select the div on which we want to act           
        .selectAll(".axis")              // This is a standard D3 mechanism to
        .data(["top"])                   // bind data to a graph. In this case
        .enter()                         // we're binding the axes "top" and "bottom".
        .append("div")                   // Create two divs and 
        .attr("class", function(d) {     // give them the classes
            return d + " axis";            // top axis and bottom axis
        })                               // respectively 
    .each(function(d) {              // For each of these axes,
        d3.select(this)                // draw the axes with 4 intervals
        .call(context.axis()         // and place them in their proper places
            .ticks(4).orient(d)); 
    });


    d3.select("#timeseries")                 
        .selectAll(".horizon")           
        .data(stocks_list.map(stock))    
        .enter()                         
        .insert("div", ".bottom")        // Insert the graph in a div  
        .attr("class", "horizon")        // Turn the div into
        .call(context.horizon()          // a horizon graph
                .height(100)
                .extent([0, 100])
                .scale(d3.scale.linear().domain([0,10]).range([0,10]))
                .format(d3.format(".2f"))    // Format the values to 2 floating-point decimals
             );


    context.on("focus", function(i) {
        d3.selectAll(".value").style("right",                  // Make the rule coincide 
            i == null ? null : context.size() - i + "px") // with the mouse
            .style("bottom",
                i == null ? null: "-22px");
    });
}

    d3.csv('data/lastDate.csv', function(error, data) {
        data.forEach(function(d) {
            lastDate = d3.time.format("%Y-%m-%d").parse(d.lastDate);
        });
        console.log(lastDate);

    context = cubism.context()
    .serverDelay(Date . now() - lastDate)
    .step(3600 * 24 * 1000) // Distance between data points in milliseconds
    .size(1024) // Number of data points
    .stop();   // Fetching from a static data source; don't update values

    d3.select("#ts-holder").append("div") // Add a vertical rule
    .attr("class", "rule")        // to the graph
    .call(context.rule());

    //draw_graph([["La Mana à Saut Sabbat", "q_saut_sabbat.json"], ["La Comté à Saut Bief", "q_saut_bief.json"], ["L'Approuague à Saut Athanase", "q_saut_athanase.json"], ["L'Oyapock à Saut Maripa", "q_saut_maripa.json"], ["La Lawa à Maripasoula", "q_maripasoula.json"], ["Le Maroni à Langa Tabiki", "q_langa_tabiki.json"], ["La Tapanahoni près de l'île Stoelmans", "q_tapanahoni.json"]]);
    //draw_graph([["La Mana à Saut Sabbat", "q_saut_sabbat.json"], ["La Comté à Saut Bief", "q_saut_bief.json"], ["L'Approuague à Saut Athanase", "q_saut_athanase.json"], ["L'Oyapock à Saut Maripa", "q_saut_maripa.json"], ["La Lawa à Maripasoula", "q_maripasoula.json"], ["Le Maroni à Langa Tabiki", "q_langa_tabiki.json"]]);
    draw_graph([[document.getElementById('river0').value, document.getElementById('river0File').value], [document.getElementById('river1').value, document.getElementById('river1File').value], [document.getElementById('river2').value, document.getElementById('river2File').value], [document.getElementById('river3').value, document.getElementById('river3File').value], [document.getElementById('river4').value, document.getElementById('river4File').value], [document.getElementById('river5').value, document.getElementById('river5File').value]]);
    });
})();
